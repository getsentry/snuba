use crate::processors::spans::SpanStatus;
use crate::types::{BadMessage, BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> Result<BytesInsertBatch, BadMessage> {
    if let Some(payload_bytes) = payload.payload {
        let msg: FromFunctionsMessage = serde_json::from_slice(&payload_bytes).map_err(|err| {
            log::error!("Failed to deserialize message: {}", err);
            BadMessage
        })?;

        let profile_id = Uuid::parse_str(msg.profile_id.as_str()).map_err(|_err| BadMessage)?;
        let timestamp = match msg.timestamp {
            Some(timestamp) => timestamp,
            _ => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_err| BadMessage)?
                .as_secs(),
        };
        let device_classification = match msg.device_class {
            Some(device_classification) => device_classification,
            _ => 0,
        };
        let mut rows = Vec::with_capacity(msg.functions.len());

        for from in &msg.functions {
            let function = Function {
                // Profile metadata
                browser_name: msg.browser_name.clone(),
                device_classification,
                dist: msg.dist.clone(),
                environment: msg.environment.clone(),
                http_method: msg.http_method.clone(),
                platform: msg.platform.clone(),
                profile_id: profile_id.to_string(),
                project_id: msg.project_id,
                release: msg.release.clone(),
                retention_days: msg.retention_days,
                timestamp,
                transaction_name: msg.transaction_name.clone(),
                transaction_op: msg.transaction_op.clone(),
                transaction_status: msg.transaction_status as u8,

                // Function metadata
                fingerprint: from.fingerprint,
                durations: from.self_times_ns.clone(),
                function: from.function.clone(),
                package: from.package.clone(),
                name: from.function.clone(),
                is_application: from.in_app as u8,

                ..Default::default()
            };
            let serialized = serde_json::to_vec(&function).map_err(|err| {
                log::error!("Failed to serialize message: {}", err);
                BadMessage
            })?;
            rows.push(serialized);
        }

        return Ok(BytesInsertBatch { rows });
    }
    Err(BadMessage)
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
    profile_id: String,
    project_id: u64,
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
    profile_id: String,
    project_id: u64,
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
        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some(data.as_bytes().to_vec()),
        };
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
