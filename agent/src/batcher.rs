use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{interval_at, Instant, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::client::{BatchSender, DeliveryError};
use crate::metrics::Metrics;
use crate::parser::LogEvent;
use crate::retry::RetryPolicy;

#[derive(Debug, Clone, Copy)]
pub struct BatcherConfig {
    pub max_size: usize,
    pub flush_interval: Duration,
}

pub async fn run_batcher<S: BatchSender>(
    mut receiver: mpsc::Receiver<LogEvent>,
    sender: S,
    config: BatcherConfig,
    retry_policy: RetryPolicy,
    metrics: Metrics,
) {
    let first_tick = Instant::now() + config.flush_interval;
    let mut ticker = interval_at(first_tick, config.flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut batch = Vec::with_capacity(config.max_size);

    loop {
        tokio::select! {
            event = receiver.recv() => {
                match event {
                    Some(event) => {
                        batch.push(event);
                        if batch.len() >= config.max_size {
                            flush(&mut batch, &sender, retry_policy, &metrics).await;
                        }
                    }
                    None => {
                        if !batch.is_empty() {
                            flush(&mut batch, &sender, retry_policy, &metrics).await;
                        }
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&mut batch, &sender, retry_policy, &metrics).await;
                }
            }
        }
    }
}

async fn flush<S: BatchSender>(
    batch: &mut Vec<LogEvent>,
    sender: &S,
    retry_policy: RetryPolicy,
    metrics: &Metrics,
) {
    let events = std::mem::take(batch);
    let count = events.len();
    for attempt in 1..=retry_policy.max_attempts {
        match sender.send_batch(&events).await {
            Ok(result) => {
                metrics.incr_batches_sent();
                if result.rejected > 0 {
                    metrics.incr_delivery_dropped_by(result.rejected as u64);
                    warn!(
                        accepted = result.accepted,
                        rejected = result.rejected,
                        "batch partially rejected"
                    );
                } else {
                    info!(accepted = result.accepted, "batch sent");
                }
                return;
            }
            Err(error) if error.is_retryable() && attempt < retry_policy.max_attempts => {
                metrics.incr_retries();
                let delay = retry_policy.delay_for_attempt(attempt);
                warn!(
                    attempt,
                    delay_ms = delay.as_millis(),
                    error = %error,
                    "batch send failed; retrying"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                record_failed_batch(metrics, count, attempt, error);
                return;
            }
        }
    }
}

fn record_failed_batch(metrics: &Metrics, count: usize, attempt: usize, error: DeliveryError) {
    metrics.incr_batches_failed();
    metrics.incr_delivery_dropped_by(count as u64);
    error!(attempt, dropped = count, error = %error, "batch permanently failed");
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::{Map, Value};

    use super::*;
    use crate::client::IngestResponse;

    #[derive(Clone)]
    struct FakeSender {
        state: Arc<Mutex<FakeState>>,
    }

    struct FakeState {
        attempts: Vec<Vec<String>>,
        outcomes: VecDeque<Result<IngestResponse, DeliveryError>>,
    }

    impl FakeSender {
        fn succeeding() -> Self {
            Self::with_outcomes(VecDeque::new())
        }

        fn with_outcomes(outcomes: VecDeque<Result<IngestResponse, DeliveryError>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeState {
                    attempts: Vec::new(),
                    outcomes,
                })),
            }
        }

        fn attempts(&self) -> Vec<Vec<String>> {
            self.state.lock().unwrap().attempts.clone()
        }
    }

    #[async_trait]
    impl BatchSender for FakeSender {
        async fn send_batch(&self, events: &[LogEvent]) -> Result<IngestResponse, DeliveryError> {
            let mut state = self.state.lock().unwrap();
            state
                .attempts
                .push(events.iter().map(|event| event.id.clone()).collect());
            state.outcomes.pop_front().unwrap_or(Ok(IngestResponse {
                accepted: events.len(),
                rejected: 0,
            }))
        }
    }

    fn event(id: &str) -> LogEvent {
        LogEvent {
            schema_version: 1,
            id: id.to_string(),
            service: "test".to_string(),
            level: "info".to_string(),
            message: "message".to_string(),
            timestamp: Utc::now(),
            attributes: Value::Object(Map::new()),
            source: Value::Object(Map::new()),
        }
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            jitter_ratio: 0.0,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_when_batch_reaches_max_size() {
        let (tx, rx) = mpsc::channel(4);
        let sender = FakeSender::succeeding();
        let observer = sender.clone();
        let task = tokio::spawn(run_batcher(
            rx,
            sender,
            BatcherConfig {
                max_size: 2,
                flush_interval: Duration::from_secs(60),
            },
            retry_policy(),
            Metrics::default(),
        ));

        tx.send(event("one")).await.unwrap();
        tx.send(event("two")).await.unwrap();
        tokio::task::yield_now().await;

        assert_eq!(
            observer.attempts(),
            vec![vec!["one".to_string(), "two".to_string()]]
        );
        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_low_volume_batch_on_interval() {
        let (tx, rx) = mpsc::channel(4);
        let sender = FakeSender::succeeding();
        let observer = sender.clone();
        let task = tokio::spawn(run_batcher(
            rx,
            sender,
            BatcherConfig {
                max_size: 10,
                flush_interval: Duration::from_secs(5),
            },
            retry_policy(),
            Metrics::default(),
        ));

        tx.send(event("one")).await.unwrap();
        tokio::task::yield_now().await;
        assert!(observer.attempts().is_empty());

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(observer.attempts(), vec![vec!["one".to_string()]]);

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn retry_reuses_the_same_event_id() {
        let outcomes = VecDeque::from([
            Err(DeliveryError::Transient("unavailable".to_string())),
            Ok(IngestResponse {
                accepted: 1,
                rejected: 0,
            }),
        ]);
        let (tx, rx) = mpsc::channel(1);
        let sender = FakeSender::with_outcomes(outcomes);
        let observer = sender.clone();
        let task = tokio::spawn(run_batcher(
            rx,
            sender,
            BatcherConfig {
                max_size: 1,
                flush_interval: Duration::from_secs(60),
            },
            retry_policy(),
            Metrics::default(),
        ));

        tx.send(event("stable-id")).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        assert_eq!(
            observer.attempts(),
            vec![vec!["stable-id".to_string()], vec!["stable-id".to_string()]]
        );
        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn closing_input_performs_a_final_flush() {
        let (tx, rx) = mpsc::channel(2);
        let sender = FakeSender::succeeding();
        let observer = sender.clone();
        let task = tokio::spawn(run_batcher(
            rx,
            sender,
            BatcherConfig {
                max_size: 10,
                flush_interval: Duration::from_secs(60),
            },
            retry_policy(),
            Metrics::default(),
        ));

        tx.send(event("final")).await.unwrap();
        drop(tx);
        task.await.unwrap();

        assert_eq!(observer.attempts(), vec![vec!["final".to_string()]]);
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_is_not_retried() {
        let outcomes = VecDeque::from([Err(DeliveryError::Permanent { status: 400 })]);
        let (tx, rx) = mpsc::channel(1);
        let sender = FakeSender::with_outcomes(outcomes);
        let observer = sender.clone();
        let metrics = Metrics::default();
        let task = tokio::spawn(run_batcher(
            rx,
            sender,
            BatcherConfig {
                max_size: 1,
                flush_interval: Duration::from_secs(60),
            },
            retry_policy(),
            metrics.clone(),
        ));

        tx.send(event("invalid")).await.unwrap();
        drop(tx);
        task.await.unwrap();

        assert_eq!(observer.attempts().len(), 1);
        assert_eq!(metrics.snapshot().delivery_dropped, 1);
        assert_eq!(metrics.snapshot().retries, 0);
    }
}
