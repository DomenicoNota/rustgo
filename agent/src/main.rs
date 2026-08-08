use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Result};
use clap::Parser;
use logstream_agent::batcher::{run_batcher, BatcherConfig};
use logstream_agent::client::BackendClient;
use logstream_agent::config::Config;
use logstream_agent::metrics::Metrics;
use logstream_agent::observability::Server as ObservabilityServer;
use logstream_agent::retry::RetryPolicy;
use logstream_agent::tailer::run_tailer;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout_at, Instant};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long, default_value = "agent.yaml")]
    config: PathBuf,
}

struct TaskExit {
    component: String,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let args = Args::parse();
    let config = Config::from_file(&args.config)?;
    let metrics = Metrics::default();
    let (sender, receiver) = mpsc::channel(config.queue_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
    let observability_server = ObservabilityServer::bind(&config.observability_bind).await?;
    let observability_exit_tx = task_exit_tx.clone();
    let observability_metrics = metrics.clone();
    let observability_shutdown = shutdown_rx.clone();
    let mut observability = tokio::spawn(async move {
        let result = observability_server
            .run(observability_metrics, observability_shutdown)
            .await;
        report_exit(
            &observability_exit_tx,
            "observability server",
            result.as_ref().err().map(ToString::to_string),
        );
        result
    });

    let client = BackendClient::new(
        &config.backend.url,
        &config.backend.api_key,
        Duration::from_millis(config.backend.request_timeout_ms),
    )?;
    let retry_policy = RetryPolicy {
        max_attempts: config.retry.max_attempts,
        base_delay: Duration::from_millis(config.retry.base_delay_ms),
        max_delay: Duration::from_millis(config.retry.max_delay_ms),
        jitter_ratio: config.retry.jitter_ratio,
    };

    let batcher_exit_tx = task_exit_tx.clone();
    let batcher_metrics = metrics.clone();
    let batcher_config = BatcherConfig {
        max_size: config.batch.max_size,
        flush_interval: Duration::from_millis(config.batch.flush_interval_ms),
    };
    let mut batcher = tokio::spawn(async move {
        run_batcher(
            receiver,
            client,
            batcher_config,
            retry_policy,
            batcher_metrics,
        )
        .await;
        report_exit(&batcher_exit_tx, "batcher", None);
    });

    let mut tailers = Vec::with_capacity(config.logs.len());
    for log in config.logs.clone() {
        let component = format!("tailer for {}", log.service);
        let tailer_sender = sender.clone();
        let tailer_metrics = metrics.clone();
        let agent_id = config.agent_id.clone();
        let shutdown = shutdown_rx.clone();
        let max_line_bytes = config.max_line_bytes;
        let tailer_exit_tx = task_exit_tx.clone();
        let exit_component = component.clone();
        let task = tokio::spawn(async move {
            let result = run_tailer(
                log,
                agent_id,
                tailer_sender,
                tailer_metrics,
                max_line_bytes,
                shutdown,
            )
            .await;
            report_exit(
                &tailer_exit_tx,
                &exit_component,
                result.as_ref().err().map(ToString::to_string),
            );
            result
        });
        tailers.push((component, task));
    }
    drop(sender);
    drop(task_exit_tx);

    info!(
        agent_id = config.agent_id,
        files = config.logs.len(),
        queue_capacity = config.queue_capacity,
        observability_bind = config.observability_bind,
        "agent started"
    );
    let mut failure = tokio::select! {
        result = wait_for_shutdown() => {
            result.err()
        }
        exit = task_exit_rx.recv() => {
            match exit {
                Some(exit) => {
                    let error = unexpected_exit_error(&exit);
                    error!(component = exit.component, error = %error, "agent component exited unexpectedly");
                    Some(error)
                }
                None => Some(anyhow!("task monitor closed before shutdown")),
            }
        }
    };
    let _ = shutdown_tx.send(true);

    // Tailers must exit first so their senders close; channel closure tells the batcher to final-flush.
    let deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
    let mut graceful = true;
    for (component, tailer) in &mut tailers {
        match timeout_at(deadline, tailer).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                error!(component, error = %error, "tailer stopped with an error");
                if failure.is_none() {
                    failure = Some(anyhow!("{component} failed: {error}"));
                }
            }
            Ok(Err(error)) => {
                error!(component, error = %error, "tailer task failed");
                if failure.is_none() {
                    failure = Some(anyhow!("{component} task failed: {error}"));
                }
            }
            Err(_) => {
                graceful = false;
                break;
            }
        }
    }

    if graceful {
        match timeout_at(deadline, &mut batcher).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                graceful = false;
                error!(error = %error, "batcher task failed");
                if failure.is_none() {
                    failure = Some(anyhow!("batcher task failed: {error}"));
                }
            }
            Err(_) => graceful = false,
        }
    }

    if graceful {
        match timeout_at(deadline, &mut observability).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                graceful = false;
                error!(error = %error, "observability server stopped with an error");
                if failure.is_none() {
                    failure = Some(anyhow!("observability server failed: {error}"));
                }
            }
            Ok(Err(error)) => {
                graceful = false;
                error!(error = %error, "observability task failed");
                if failure.is_none() {
                    failure = Some(anyhow!("observability task failed: {error}"));
                }
            }
            Err(_) => graceful = false,
        }
    }

    if !graceful {
        for (_, tailer) in &tailers {
            tailer.abort();
        }
        batcher.abort();
        observability.abort();
        warn!(
            timeout_ms = config.shutdown_timeout_ms,
            "graceful shutdown deadline exceeded; in-flight events may be lost"
        );
        if failure.is_none() {
            failure = Some(anyhow!(
                "graceful shutdown exceeded {} ms",
                config.shutdown_timeout_ms
            ));
        }
    }

    let snapshot = metrics.snapshot();
    info!(
        lines_read = snapshot.lines_read,
        batches_sent = snapshot.batches_sent,
        batches_failed = snapshot.batches_failed,
        retries = snapshot.retries,
        logs_dropped = snapshot.logs_dropped,
        queue_dropped = snapshot.queue_dropped,
        delivery_dropped = snapshot.delivery_dropped,
        oversized_line_dropped = snapshot.oversized_line_dropped,
        graceful,
        "agent stopped"
    );
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn report_exit(sender: &mpsc::UnboundedSender<TaskExit>, component: &str, error: Option<String>) {
    let _ = sender.send(TaskExit {
        component: component.to_string(),
        error,
    });
}

fn unexpected_exit_error(exit: &TaskExit) -> anyhow::Error {
    match &exit.error {
        Some(error) => anyhow!("{} failed: {}", exit.component, error),
        None => anyhow!("{} exited before shutdown", exit.component),
    }
}

#[cfg(unix)]
async fn wait_for_shutdown() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown() -> Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unexpected_exit_includes_component_and_cause() {
        let exit = TaskExit {
            component: "tailer for payments".to_string(),
            error: Some("read failed".to_string()),
        };

        assert_eq!(
            unexpected_exit_error(&exit).to_string(),
            "tailer for payments failed: read failed"
        );
    }
}
