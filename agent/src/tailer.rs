use std::fs::Metadata;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use crate::config::LogConfig;
use crate::metrics::Metrics;
use crate::parser::{parse_line, LogEvent};

const POLL_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run_tailer(
    config: LogConfig,
    agent_id: String,
    sender: mpsc::Sender<LogEvent>,
    metrics: Metrics,
    max_line_bytes: usize,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let path = PathBuf::from(config.path.clone());
    let hostname = hostname::get()
        .ok()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    let mut position = TailPosition::default();
    let mut pending = Vec::new();
    let mut pending_overflowed = false;
    let mut consecutive_open_failures = 0_u64;
    let emitter = LineEmitter {
        config: &config,
        agent_id: &agent_id,
        hostname: &hostname,
        sender: &sender,
        metrics: &metrics,
        max_line_bytes,
    };

    while !*shutdown.borrow() {
        match File::open(&path).await {
            Ok(mut file) => {
                consecutive_open_failures = 0;
                let metadata = file.metadata().await?;
                let reset = position.observe(file_identity(&metadata), metadata.len());
                if reset.requires_new_stream()
                    && (!pending.is_empty() || pending_overflowed)
                    && !emitter.finish(&mut pending, &mut pending_overflowed)
                {
                    return Ok(());
                }
                file.seek(SeekFrom::Start(position.offset)).await?;
                let mut reader = BufReader::new(file);

                loop {
                    if *shutdown.borrow() {
                        break;
                    }
                    let available = reader.fill_buf().await?;
                    if available.is_empty() {
                        break;
                    }
                    let newline = available.iter().position(|byte| *byte == b'\n');
                    let consumed = newline.map_or(available.len(), |index| index + 1);
                    append_bounded(
                        &mut pending,
                        &mut pending_overflowed,
                        &available[..consumed],
                        max_line_bytes.saturating_add(2),
                    );
                    reader.consume(consumed);
                    position.offset += consumed as u64;

                    if newline.is_some() && !emitter.finish(&mut pending, &mut pending_overflowed) {
                        return Ok(());
                    }
                }
            }
            Err(error) => {
                consecutive_open_failures = consecutive_open_failures.saturating_add(1);
                if should_report_count(consecutive_open_failures) {
                    warn!(
                        path = %path.display(),
                        attempts = consecutive_open_failures,
                        error = %error,
                        "log file unavailable; retrying"
                    );
                }
            }
        }

        debug!(
            service = config.service,
            path = config.path,
            "tailer waiting for file changes"
        );
        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }

    if !pending.is_empty() || pending_overflowed {
        emitter.finish(&mut pending, &mut pending_overflowed);
    }
    Ok(())
}

fn append_bounded(pending: &mut Vec<u8>, overflowed: &mut bool, bytes: &[u8], capacity: usize) {
    if *overflowed {
        return;
    }
    if pending.len().saturating_add(bytes.len()) > capacity {
        pending.clear();
        *overflowed = true;
        return;
    }
    pending.extend_from_slice(bytes);
}

struct LineEmitter<'a> {
    config: &'a LogConfig,
    agent_id: &'a str,
    hostname: &'a str,
    sender: &'a mpsc::Sender<LogEvent>,
    metrics: &'a Metrics,
    max_line_bytes: usize,
}

impl LineEmitter<'_> {
    fn finish(&self, pending: &mut Vec<u8>, overflowed: &mut bool) -> bool {
        self.metrics.incr_lines_read();
        let content = strip_line_ending(pending);
        if *overflowed || content.len() > self.max_line_bytes {
            let dropped = self.metrics.incr_oversized_line_dropped();
            if should_report_count(dropped) {
                warn!(
                    service = self.config.service,
                    path = self.config.path,
                    dropped,
                    max_line_bytes = self.max_line_bytes,
                    "oversized log lines dropped"
                );
            }
            pending.clear();
            *overflowed = false;
            return true;
        }

        let line = String::from_utf8_lossy(content);
        let open = if let Some(mut event) = parse_line(
            &self.config.service,
            &self.config.path,
            self.config.format,
            &line,
        ) {
            if let Value::Object(source) = &mut event.source {
                source.insert("host".to_string(), Value::String(self.hostname.to_string()));
                source.insert(
                    "agent".to_string(),
                    Value::String(self.agent_id.to_string()),
                );
            }
            enqueue(self.sender, event, self.metrics)
        } else {
            true
        };
        pending.clear();
        *overflowed = false;
        open
    }
}

fn strip_line_ending(bytes: &[u8]) -> &[u8] {
    let without_lf = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    without_lf.strip_suffix(b"\r").unwrap_or(without_lf)
}

fn enqueue(sender: &mpsc::Sender<LogEvent>, event: LogEvent, metrics: &Metrics) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            let dropped = metrics.incr_queue_dropped();
            if should_report_count(dropped) {
                warn!(dropped, "agent event queue full; events dropped");
            }
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

fn should_report_count(count: u64) -> bool {
    count == 1 || count.is_power_of_two()
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct FileIdentity(u64, u64);

fn file_identity(metadata: &Metadata) -> FileIdentity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return FileIdentity(metadata.dev(), metadata.ino());
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        return FileIdentity(metadata.creation_time(), 0);
    }

    #[allow(unreachable_code)]
    FileIdentity(0, 0)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StreamReset {
    Initial,
    Continued,
    Truncated,
    Replaced,
}

impl StreamReset {
    fn requires_new_stream(self) -> bool {
        matches!(self, Self::Truncated | Self::Replaced)
    }
}

#[derive(Debug, Default)]
struct TailPosition {
    identity: Option<FileIdentity>,
    offset: u64,
}

impl TailPosition {
    fn observe(&mut self, identity: FileIdentity, length: u64) -> StreamReset {
        let reset = match self.identity {
            None => StreamReset::Initial,
            Some(previous) if previous != identity => StreamReset::Replaced,
            Some(_) if length < self.offset => StreamReset::Truncated,
            Some(_) => StreamReset::Continued,
        };
        if reset.requires_new_stream() {
            self.offset = 0;
        }
        self.identity = Some(identity);
        reset
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::{Map, Value};

    use super::*;

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

    #[test]
    fn truncation_and_replacement_reset_the_offset() {
        let mut position = TailPosition::default();
        assert_eq!(
            position.observe(FileIdentity(1, 1), 100),
            StreamReset::Initial
        );
        position.offset = 80;
        assert_eq!(
            position.observe(FileIdentity(1, 1), 40),
            StreamReset::Truncated
        );
        assert_eq!(position.offset, 0);

        position.offset = 20;
        assert_eq!(
            position.observe(FileIdentity(2, 1), 100),
            StreamReset::Replaced
        );
        assert_eq!(position.offset, 0);
    }

    #[test]
    fn bounded_queue_counts_overflow_without_blocking() {
        let (sender, _receiver) = mpsc::channel(1);
        let metrics = Metrics::default();

        assert!(enqueue(&sender, event("one"), &metrics));
        assert!(enqueue(&sender, event("two"), &metrics));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.queue_dropped, 1);
        assert_eq!(snapshot.logs_dropped, 1);
    }

    #[test]
    fn bounded_line_buffer_discards_content_after_limit() {
        let mut pending = Vec::new();
        let mut overflowed = false;
        append_bounded(&mut pending, &mut overflowed, b"1234", 4);
        append_bounded(&mut pending, &mut overflowed, b"5", 4);
        assert!(overflowed);
        assert!(pending.is_empty());
    }

    #[test]
    fn strips_only_line_endings() {
        assert_eq!(strip_line_ending(b"  message  \r\n"), b"  message  ");
    }

    #[test]
    fn repeated_failures_are_reported_with_exponential_spacing() {
        let reported: Vec<_> = (1..=10)
            .filter(|count| should_report_count(*count))
            .collect();
        assert_eq!(reported, vec![1, 2, 4, 8]);
    }
}
