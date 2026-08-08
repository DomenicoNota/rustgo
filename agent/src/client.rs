use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Serialize;
use thiserror::Error;

use crate::parser::LogEvent;

#[derive(Clone)]
pub struct BackendClient {
    http: reqwest::Client,
    ingest_url: String,
    api_key: String,
}

impl BackendClient {
    pub fn new(
        base_url: &str,
        api_key: &str,
        request_timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let base = base_url.trim_end_matches('/');
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(request_timeout)
                .build()?,
            ingest_url: format!("{base}/v1/ingest"),
            api_key: api_key.to_string(),
        })
    }
}

#[async_trait]
pub trait BatchSender: Send + Sync {
    async fn send_batch(&self, events: &[LogEvent]) -> Result<IngestResponse, DeliveryError>;
}

#[async_trait]
impl BatchSender for BackendClient {
    async fn send_batch(&self, events: &[LogEvent]) -> Result<IngestResponse, DeliveryError> {
        let request = IngestRequest { events };
        let response = self
            .http
            .post(&self.ingest_url)
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|error| DeliveryError::Transient(error.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(classify_status(status));
        }
        response
            .json::<IngestResponse>()
            .await
            .map_err(|error| DeliveryError::Transient(format!("decode ingest response: {error}")))
    }
}

#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum DeliveryError {
    #[error("transient delivery failure: {0}")]
    Transient(String),
    #[error("permanent delivery failure: HTTP {status}")]
    Permanent { status: u16 },
}

impl DeliveryError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

fn classify_status(status: StatusCode) -> DeliveryError {
    if status.is_client_error()
        && status != StatusCode::REQUEST_TIMEOUT
        && status != StatusCode::TOO_MANY_REQUESTS
    {
        DeliveryError::Permanent {
            status: status.as_u16(),
        }
    } else {
        DeliveryError::Transient(format!("HTTP {}", status.as_u16()))
    }
}

#[derive(Serialize)]
struct IngestRequest<'a> {
    events: &'a [LogEvent],
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct IngestResponse {
    pub accepted: usize,
    pub rejected: usize,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use httpmock::prelude::*;
    use serde_json::{Map, Value};

    use super::*;

    fn event() -> LogEvent {
        LogEvent {
            schema_version: 1,
            id: "stable-id".to_string(),
            service: "test-service".to_string(),
            level: "info".to_string(),
            message: "hello".to_string(),
            timestamp: Utc::now(),
            attributes: Value::Object(Map::new()),
            source: Value::Object(Map::new()),
        }
    }

    #[test]
    fn classifies_obvious_client_failures_as_permanent() {
        assert!(!classify_status(StatusCode::BAD_REQUEST).is_retryable());
        assert!(!classify_status(StatusCode::UNAUTHORIZED).is_retryable());
        assert!(classify_status(StatusCode::REQUEST_TIMEOUT).is_retryable());
        assert!(classify_status(StatusCode::TOO_MANY_REQUESTS).is_retryable());
        assert!(classify_status(StatusCode::SERVICE_UNAVAILABLE).is_retryable());
    }

    #[tokio::test]
    async fn sends_the_authenticated_versioned_http_contract() {
        let server = MockServer::start_async().await;
        let request = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/v1/ingest")
                    .header("authorization", "Bearer test-key")
                    .body_contains(r#""schema_version":1"#)
                    .body_contains(r#""id":"stable-id""#);
                then.status(202)
                    .header("content-type", "application/json")
                    .body(r#"{"accepted":1,"rejected":0}"#);
            })
            .await;
        let client =
            BackendClient::new(&server.base_url(), "test-key", Duration::from_secs(1)).unwrap();

        let result = client.send_batch(&[event()]).await.unwrap();

        assert_eq!(result.accepted, 1);
        request.assert_async().await;
    }
}
