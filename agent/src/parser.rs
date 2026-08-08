use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::config::LogFormat;

pub const LOG_EVENT_SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LogEvent {
    pub schema_version: u8,
    pub id: String,
    pub service: String,
    pub level: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub attributes: Value,
    pub source: Value,
}

pub fn parse_line(
    service: &str,
    file_path: &str,
    format: LogFormat,
    line: &str,
) -> Option<LogEvent> {
    if line.trim().is_empty() {
        return None;
    }
    match format {
        LogFormat::Json => parse_json(service, file_path, line)
            .or_else(|| Some(plain_event(service, file_path, line, "info"))),
        LogFormat::Plain => Some(plain_event(service, file_path, line, "info")),
    }
}

fn parse_json(service: &str, file_path: &str, line: &str) -> Option<LogEvent> {
    let mut object = serde_json::from_str::<Map<String, Value>>(line).ok()?;
    let level =
        take_string(&mut object, &["level", "severity"]).unwrap_or_else(|| "info".to_string());
    let message = take_string(&mut object, &["message", "msg"]).unwrap_or_else(|| line.to_string());
    let timestamp = take_string(&mut object, &["timestamp", "time"])
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    Some(LogEvent {
        schema_version: LOG_EVENT_SCHEMA_VERSION,
        id: uuid::Uuid::new_v4().to_string(),
        service: service.to_string(),
        level: normalize_level(&level),
        message,
        timestamp,
        attributes: Value::Object(object),
        source: source(file_path),
    })
}

fn plain_event(service: &str, file_path: &str, line: &str, level: &str) -> LogEvent {
    LogEvent {
        schema_version: LOG_EVENT_SCHEMA_VERSION,
        id: uuid::Uuid::new_v4().to_string(),
        service: service.to_string(),
        level: normalize_level(level),
        message: line.to_string(),
        timestamp: Utc::now(),
        attributes: Value::Object(Map::new()),
        source: source(file_path),
    }
}

fn source(file_path: &str) -> Value {
    let mut source = Map::new();
    source.insert("file".to_string(), Value::String(file_path.to_string()));
    Value::Object(source)
}

fn take_string(object: &mut Map<String, Value>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(Value::String(text)) = object.get(*key) {
            let text = text.clone();
            object.remove(*key);
            return Some(text);
        }
    }
    None
}

fn normalize_level(level: &str) -> String {
    match level.trim().to_ascii_lowercase().as_str() {
        "warning" => "warn".to_string(),
        "trace" | "debug" | "info" | "warn" | "error" | "fatal" => {
            level.trim().to_ascii_lowercase()
        }
        _ => "info".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_log_with_level_and_message() {
        let event = parse_line(
            "auth-service",
            "auth.log",
            LogFormat::Json,
            r#"{"level":"error","message":"failed login","user_id":"123","timestamp":"2026-07-07T20:15:00Z"}"#,
        )
        .unwrap();
        assert_eq!(event.level, "error");
        assert_eq!(event.schema_version, LOG_EVENT_SCHEMA_VERSION);
        assert_eq!(event.message, "failed login");
        assert_eq!(event.attributes["user_id"], "123");
        assert!(!event.id.is_empty());
    }

    #[test]
    fn parse_plain_text_log_defaults_to_info() {
        let event = parse_line(
            "payment",
            "payment.log",
            LogFormat::Plain,
            "  payment retry  ",
        )
        .unwrap();
        assert_eq!(event.level, "info");
        assert_eq!(event.message, "  payment retry  ");
    }

    #[test]
    fn malformed_json_is_preserved_as_plain_text() {
        let line = r#"{"message":"unterminated"#;
        let event = parse_line("api", "api.log", LogFormat::Json, line).unwrap();
        assert_eq!(event.message, line);
        assert_eq!(event.attributes, Value::Object(Map::new()));
    }

    #[test]
    fn non_string_well_known_fields_remain_attributes() {
        let event = parse_line(
            "api",
            "api.log",
            LogFormat::Json,
            r#"{"message":42,"level":{"name":"warn"},"timestamp":123}"#,
        )
        .unwrap();

        assert_eq!(event.level, "info");
        assert_eq!(
            event.message,
            r#"{"message":42,"level":{"name":"warn"},"timestamp":123}"#
        );
        assert_eq!(event.attributes["message"], 42);
        assert!(event.attributes.get("level").is_some());
        assert_eq!(event.attributes["timestamp"], 123);
    }
}
