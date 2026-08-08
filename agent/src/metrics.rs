use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct Metrics {
    lines_read: Arc<AtomicU64>,
    batches_sent: Arc<AtomicU64>,
    batches_failed: Arc<AtomicU64>,
    retries: Arc<AtomicU64>,
    logs_dropped: Arc<AtomicU64>,
    queue_dropped: Arc<AtomicU64>,
    delivery_dropped: Arc<AtomicU64>,
    oversized_line_dropped: Arc<AtomicU64>,
}

impl Metrics {
    pub fn incr_lines_read(&self) {
        self.lines_read.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_batches_sent(&self) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_batches_failed(&self) {
        self.batches_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_retries(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_queue_dropped(&self) -> u64 {
        self.logs_dropped.fetch_add(1, Ordering::Relaxed);
        self.queue_dropped.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn incr_delivery_dropped_by(&self, count: u64) {
        self.logs_dropped.fetch_add(count, Ordering::Relaxed);
        self.delivery_dropped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_oversized_line_dropped(&self) -> u64 {
        self.logs_dropped.fetch_add(1, Ordering::Relaxed);
        self.oversized_line_dropped.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            lines_read: self.lines_read.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            batches_failed: self.batches_failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            logs_dropped: self.logs_dropped.load(Ordering::Relaxed),
            queue_dropped: self.queue_dropped.load(Ordering::Relaxed),
            delivery_dropped: self.delivery_dropped.load(Ordering::Relaxed),
            oversized_line_dropped: self.oversized_line_dropped.load(Ordering::Relaxed),
        }
    }

    pub fn render_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let counters = [
            ("logstream_agent_lines_read_total", snapshot.lines_read),
            ("logstream_agent_batches_sent_total", snapshot.batches_sent),
            (
                "logstream_agent_batches_failed_total",
                snapshot.batches_failed,
            ),
            ("logstream_agent_retries_total", snapshot.retries),
            ("logstream_agent_logs_dropped_total", snapshot.logs_dropped),
            (
                "logstream_agent_queue_dropped_total",
                snapshot.queue_dropped,
            ),
            (
                "logstream_agent_delivery_dropped_total",
                snapshot.delivery_dropped,
            ),
            (
                "logstream_agent_oversized_lines_dropped_total",
                snapshot.oversized_line_dropped,
            ),
        ];
        counters
            .into_iter()
            .map(|(name, value)| format!("# TYPE {name} counter\n{name} {value}\n"))
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub lines_read: u64,
    pub batches_sent: u64,
    pub batches_failed: u64,
    pub retries: u64,
    pub logs_dropped: u64,
    pub queue_dropped: u64,
    pub delivery_dropped: u64,
    pub oversized_line_dropped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_output_reports_live_counters_without_labels() {
        let metrics = Metrics::default();
        metrics.incr_retries();
        metrics.incr_queue_dropped();
        let output = metrics.render_prometheus();
        assert!(output.contains("logstream_agent_retries_total 1"));
        assert!(output.contains("logstream_agent_logs_dropped_total 1"));
        assert!(output.contains("logstream_agent_queue_dropped_total 1"));
        assert!(!output.contains('{'));
    }
}
