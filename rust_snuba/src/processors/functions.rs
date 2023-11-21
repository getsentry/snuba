use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processors::spans::SpanStatus;
use crate::types::{KafkaMessageMetadata, RowData};

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> anyhow::Result<RowData> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromFunctionsMessage = serde_json::from_slice(payload_bytes)?;

    let timestamp = match msg.timestamp {
        Some(timestamp) => timestamp,
        _ => SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    };
    let device_classification = msg.device_class.unwrap_or_default();

    let mut rows = Vec::with_capacity(msg.functions.len());
    for from in msg.functions {
        let function = Function {
            profile_id: msg.profile_id,
            project_id: msg.project_id,
            // Profile metadata
            browser_name: msg.browser_name.clone(),
            device_classification,
            dist: msg.dist.clone(),
            environment: msg.environment.clone(),
            http_method: msg.http_method.clone(),
            platform: msg.platform.clone(),
            release: msg.release.clone(),
            retention_days: msg.retention_days,
            timestamp,
            transaction_name: msg.transaction_name.clone(),
            transaction_op: msg.transaction_op.clone(),
            transaction_status: msg.transaction_status as u8,

            // Function metadata
            fingerprint: from.fingerprint,
            durations: from.self_times_ns,
            function: from.function.clone(),
            package: from.package.clone(),
            name: from.function,
            is_application: from.in_app as u8,

            ..Default::default()
        };
        let serialized = serde_json::to_vec(&function)?;
        rows.push(serialized);
    }

    Ok(RowData { rows })
}

#[derive(Debug, Deserialize)]
struct FromFunction {
    fingerprint: u64,
    function: String,
    in_app: bool,
    package: String,
    self_times_ns: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct FromFunctionsMessage {
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
    functions: Vec<FromFunction>,
    #[serde(default)]
    http_method: Option<String>,
    platform: String,
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
struct Function {
    profile_id: Uuid,
    project_id: u64,
    browser_name: Option<String>,
    device_classification: u32,
    dist: Option<String>,
    durations: Vec<u64>,
    environment: Option<String>,
    fingerprint: u64,
    function: String,
    http_method: Option<String>,
    is_application: u8,
    materialization_version: u8,
    module: String,
    name: String,
    package: String,
    platform: String,
    release: Option<String>,
    retention_days: u32,
    timestamp: u64,
    transaction_name: String,
    transaction_op: String,
    transaction_status: u8,

    // Deprecated fields
    depth: u8,
    os_name: String,
    os_version: String,
    parent_fingerprint: u8,
    path: String,
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
