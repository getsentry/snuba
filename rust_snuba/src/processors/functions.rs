use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: InputMessage = serde_json::from_slice(payload_bytes)?;

    let functions: Vec<Function> = msg
        .functions
        .iter()
        .map(|from| {
            Function {
                profile_id: msg.profile_id,
                project_id: msg.project_id,

                // Profile metadata
                environment: msg.environment.as_deref(),
                platform: &msg.platform,
                release: msg.release.as_deref(),
                retention_days: msg.retention_days,
                timestamp: msg.timestamp,
                transaction_name: &msg.transaction_name,

                // Function metadata
                fingerprint: from.fingerprint,
                durations: &from.self_times_ns,
                package: &from.package,
                name: &from.function,
                is_application: from.in_app as u8,

                ..Default::default()
            }
        })
        .collect();

    InsertBatch::from_rows(functions, DateTime::from_timestamp(msg.received, 0))
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
    #[serde(default)]
    environment: Option<String>,
    functions: Vec<InputFunction>,
    platform: String,
    profile_id: Uuid,
    project_id: u64,
    received: i64,
    #[serde(default)]
    release: Option<String>,
    retention_days: u32,
    timestamp: u64,
    transaction_name: String,
}

#[derive(Default, Debug, Serialize)]
struct Function<'a> {
    durations: &'a [u64],
    environment: Option<&'a str>,
    fingerprint: u64,
    is_application: u8,
    materialization_version: u8,
    name: &'a str,
    package: &'a str,
    platform: &'a str,
    profile_id: Uuid,
    project_id: u64,
    release: Option<&'a str>,
    retention_days: u32,
    timestamp: u64,
    transaction_name: &'a str,

    // Deprecated fields
    browser_name: &'a str,
    depth: u8,
    device_classification: u32,
    dist: &'a str,
    os_name: &'a str,
    os_version: &'a str,
    parent_fingerprint: u8,
    path: &'a str,
    transaction_op: &'a str,
    transaction_status: u8,
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
            "received": 1694447692,
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
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }
}
