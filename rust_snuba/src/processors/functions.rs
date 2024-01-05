use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use chrono::DateTime;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processors::spans::SpanStatus;
use crate::types::{InsertBatch, KafkaMessageMetadata, RowData};

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: InputMessage = serde_json::from_slice(payload_bytes)?;

    let timestamp = match msg.timestamp {
        Some(timestamp) => timestamp,
        _ => SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    };
    let device_classification = msg.device_class.unwrap_or_default();

    let functions = msg.functions.iter().map(|from| {
        Function {
            profile_id: msg.profile_id,
            project_id: msg.project_id,

            // Profile metadata
            browser_name: msg.browser_name.as_deref(),
            device_classification,
            dist: msg.dist.as_deref(),
            environment: msg.environment.as_deref(),
            http_method: msg.http_method.as_deref(),
            platform: &msg.platform,
            release: msg.release.as_deref(),
            retention_days: msg.retention_days,
            timestamp,
            transaction_name: &msg.transaction_name,
            transaction_op: &msg.transaction_op,
            transaction_status: msg.transaction_status as u8,

            // Function metadata
            fingerprint: from.fingerprint,
            durations: &from.self_times_ns,
            function: &from.function,
            package: &from.package,
            name: &from.function,
            is_application: from.in_app as u8,

            ..Default::default()
        }
    });

    let origin_timestamp = match msg.received {
        Some(origin_timestamp) => DateTime::from_timestamp(origin_timestamp, 0),
        _ => None,
    };

    Ok(InsertBatch {
        rows: RowData::from_rows(functions)?,
        origin_timestamp,
        sentry_received_timestamp: None,
    })
}

#[derive(Debug, Deserialize)]
struct InputFunction {
    fingerprint: u64,
    function: String,
    in_app: bool,
    package: String,
    self_times_ns: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct InputMessage {
    profile_id: Uuid,
    project_id: u64,
    #[serde(default)]
    browser_name: Option<String>,
    #[serde(default)]
    device_class: Option<u32>,
    #[serde(default)]
    dist: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    functions: Vec<InputFunction>,
    #[serde(default)]
    http_method: Option<String>,
    platform: String,
    received: Option<i64>,
    #[serde(default)]
    release: Option<String>,
    retention_days: u32,
    #[serde(default)]
    timestamp: Option<u64>,
    transaction_name: String,
    transaction_op: String,
    transaction_status: SpanStatus,
}

#[derive(Default, Debug, Serialize)]
struct Function<'a> {
    profile_id: Uuid,
    project_id: u64,
    browser_name: Option<&'a str>,
    device_classification: u32,
    dist: Option<&'a str>,
    durations: &'a [u64],
    environment: Option<&'a str>,
    fingerprint: u64,
    function: &'a str,
    http_method: Option<&'a str>,
    is_application: u8,
    materialization_version: u8,
    module: &'a str,
    name: &'a str,
    package: &'a str,
    platform: &'a str,
    release: Option<&'a str>,
    retention_days: u32,
    timestamp: u64,
    transaction_name: &'a str,
    transaction_op: &'a str,
    transaction_status: u8,

    // Deprecated fields
    depth: u8,
    os_name: &'a str,
    os_version: &'a str,
    parent_fingerprint: u8,
    path: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

    #[test]
    fn test_functions() {
        let data = r#"{
            "project_id": 22,
            "profile_id": "7329158c39964fbb9ec57c20cf4a2bb8",
            "transaction_name": "vroom-vroom",
            "timestamp": 1694447692,
            "functions": [
                {
                    "fingerprint": 123,
                    "function": "foo",
                    "package": "bar",
                    "in_app": true,
                    "self_times_ns": [1, 2, 3]
                },
                {
                    "fingerprint": 456,
                    "function": "baz",
                    "package": "qux",
                    "in_app": false,
                    "self_times_ns": [4, 5, 6]
                }
            ],
            "platform": "python",
            "environment": "prod",
            "release": "foo@1.0.0",
            "dist": "1",
            "transaction_op": "http.server",
            "transaction_status": "ok",
            "http_method": "GET",
            "browser_name": "Chrome",
            "device_class": 2,
            "retention_days": 30
        }"#;
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
