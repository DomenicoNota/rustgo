use anyhow::{Context, Result};
use serde::Deserialize;
use std::{env, fs, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub agent_id: String,
    pub backend: BackendConfig,
    pub logs: Vec<LogConfig>,
    pub batch: BatchConfig,
    pub retry: RetryConfig,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
    #[serde(default = "default_max_line_bytes")]
    pub max_line_bytes: usize,
    #[serde(default = "default_observability_bind")]
    pub observability_bind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    pub url: String,
    pub api_key: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    pub service: String,
    pub path: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Plain,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchConfig {
    pub max_size: usize,
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub base_delay_ms: u64,
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
    #[serde(default = "default_jitter_ratio")]
    pub jitter_ratio: f64,
}

fn default_queue_capacity() -> usize {
    10_000
}

fn default_shutdown_timeout_ms() -> u64 {
    10_000
}

fn default_request_timeout_ms() -> u64 {
    10_000
}

fn default_max_line_bytes() -> usize {
    256 * 1024
}

fn default_max_delay_ms() -> u64 {
    5_000
}

fn default_jitter_ratio() -> f64 {
    0.2
}

fn default_observability_bind() -> String {
    "127.0.0.1:9090".to_owned()
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw =
            fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
        let mut config: Config = serde_yaml::from_str(&raw).context("parse YAML config")?;
        override_from_env(&mut config.agent_id, "LOGSTREAM_AGENT_ID");
        override_from_env(&mut config.backend.url, "LOGSTREAM_BACKEND_URL");
        override_from_env(&mut config.backend.api_key, "LOGSTREAM_API_KEY");
        override_from_env(
            &mut config.observability_bind,
            "LOGSTREAM_OBSERVABILITY_BIND",
        );
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.agent_id.trim().is_empty(), "agent_id is required");
        anyhow::ensure!(
            !self.backend.url.trim().is_empty(),
            "backend.url is required"
        );
        anyhow::ensure!(
            !self.backend.api_key.trim().is_empty(),
            "backend.api_key is required"
        );
        anyhow::ensure!(
            self.backend.request_timeout_ms > 0,
            "backend.request_timeout_ms must be positive"
        );
        anyhow::ensure!(!self.logs.is_empty(), "at least one log file is required");
        anyhow::ensure!(self.queue_capacity > 0, "queue_capacity must be positive");
        anyhow::ensure!(self.max_line_bytes > 0, "max_line_bytes must be positive");
        self.observability_bind
            .parse::<std::net::SocketAddr>()
            .context("observability_bind must be an IP address and port")?;
        anyhow::ensure!(
            self.shutdown_timeout_ms > 0,
            "shutdown_timeout_ms must be positive"
        );
        anyhow::ensure!(self.batch.max_size > 0, "batch.max_size must be positive");
        anyhow::ensure!(
            self.batch.flush_interval_ms > 0,
            "batch.flush_interval_ms must be positive"
        );
        anyhow::ensure!(
            self.retry.max_attempts > 0,
            "retry.max_attempts must be positive"
        );
        anyhow::ensure!(
            self.retry.base_delay_ms > 0,
            "retry.base_delay_ms must be positive"
        );
        anyhow::ensure!(
            self.retry.max_delay_ms >= self.retry.base_delay_ms,
            "retry.max_delay_ms must be at least retry.base_delay_ms"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.retry.jitter_ratio),
            "retry.jitter_ratio must be between 0 and 1"
        );
        Ok(())
    }
}

fn override_from_env(target: &mut String, name: &str) {
    if let Ok(value) = env::var(name) {
        if !value.trim().is_empty() {
            *target = value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_config() {
        let raw = r#"
agent_id: local
backend:
  url: http://localhost:8080
  api_key: local-only-example-key
logs:
  - service: auth
    path: auth.log
    format: json
batch:
  max_size: 10
  flush_interval_ms: 1000
retry:
  max_attempts: 3
  base_delay_ms: 100
"#;
        let config: Config = serde_yaml::from_str(raw).unwrap();
        config.validate().unwrap();
        assert_eq!(config.logs[0].format, LogFormat::Json);
        assert_eq!(config.queue_capacity, 10_000);
        assert_eq!(config.backend.request_timeout_ms, 10_000);
        assert_eq!(config.retry.max_delay_ms, 5_000);
        assert_eq!(config.retry.jitter_ratio, 0.2);
    }
}
