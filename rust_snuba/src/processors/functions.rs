use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use sentry_arroyo::backends::kafka::types::KafkaPayload;

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
                start_timestamp: msg.start_timestamp.map(|t| (t * 1e6) as u64),
                end_timestamp: msg.end_timestamp.map(|t| (t * 1e6) as u64),
                profiling_type: msg.profiling_type.as_deref(),
                materialization_version: msg.materialization_version.unwrap_or_default(),

                // Function metadata
                fingerprint: from.fingerprint,
                durations: &from.self_times_ns,
                package: &from.package,
                name: &from.function,
                is_application: from.in_app as u8,
                thread_id: from.thread_id.clone().unwrap_or_default(),

                ..Default::default()
            }
        })
        .collect();

    InsertBatch::from_rows(functions, DateTime::from_timestamp(msg.received, 0))
}

#[derive(Debug, Deserialize, JsonSchema)]
struct InputFunction {
    fingerprint: u64,
    function: String,
    in_app: bool,
    package: String,
    self_times_ns: Vec<u64>,
    #[serde(default)]
    thread_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
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
    #[serde(default)]
    start_timestamp: Option<f64>,
    #[serde(default)]
    end_timestamp: Option<f64>,
    transaction_name: String,
    #[serde(default)]
    profiling_type: Option<String>,
    #[serde(default)]
    materialization_version: Option<u8>,
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
    start_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    transaction_name: &'a str,
    thread_id: String,
    profiling_type: Option<&'a str>,

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
    use std::time::SystemTime;

    use crate::processors::tests::run_schema_type_test;

    use super::*;

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

    #[test]
    fn schema() {
        run_schema_type_test::<InputMessage>("profiles-call-tree", None);
    }
}
