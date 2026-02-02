use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use schemars::JsonSchema;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: InputMessage = serde_json::from_slice(payload_bytes)?;

    let retention_days = Some(enforce_retention(msg.retention_days, &config.env_config));
    let origin_timestamp = DateTime::from_timestamp(msg.received, 0);

    let row = Profile {
        android_api_level: msg.android_api_level,
        architecture: msg.architecture,
        device_classification: msg.device_classification,
        device_locale: msg.device_locale,
        device_manufacturer: msg.device_manufacturer,
        device_model: msg.device_model,
        device_os_build_number: msg.device_os_build_number,
        device_os_name: msg.device_os_name,
        device_os_version: msg.device_os_version,
        duration_ns: msg.duration_ns,
        environment: msg.environment,
        organization_id: msg.organization_id,
        platform: msg.platform,
        profile_id: msg.profile_id,
        project_id: msg.project_id,
        received: msg.received,
        retention_days,
        trace_id: msg.trace_id,
        transaction_id: msg.transaction_id,
        transaction_name: msg.transaction_name,
        version_code: msg.version_code,
        version_name: msg.version_name,
        offset: metadata.offset,
        partition: metadata.partition,
    };

    InsertBatch::from_rows([row], origin_timestamp)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct InputMessage {
    #[serde(default)]
    android_api_level: Option<u32>,
    #[serde(default)]
    architecture: Option<String>,
    #[serde(default)]
    device_classification: String,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    #[serde(default)]
    device_os_build_number: Option<String>,
    device_os_name: String,
    device_os_version: String,
    duration_ns: u64,
    #[serde(default)]
    environment: Option<String>,
    organization_id: u64,
    platform: String,
    profile_id: Uuid,
    project_id: u64,
    received: i64,
    retention_days: Option<u16>,
    trace_id: Uuid,
    transaction_id: Uuid,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

#[derive(Debug, Serialize, JsonSchema, clickhouse::Row)]
struct Profile {
    android_api_level: Option<u32>,
    architecture: Option<String>,
    device_classification: String,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_build_number: Option<String>,
    device_os_name: String,
    device_os_version: String,
    duration_ns: u64,
    environment: Option<String>,
    organization_id: u64,
    platform: String,
    profile_id: Uuid,
    project_id: u64,
    received: i64,
    retention_days: Option<u16>,
    trace_id: Uuid,
    transaction_id: Uuid,
    transaction_name: String,
    version_code: String,
    version_name: String,
    offset: u64,
    partition: u16,
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::processors::tests::run_schema_type_test;

    use super::*;

    #[test]
    fn test_profile() {
        let data = r#"{
            "android_api_level": null,
            "architecture": "aarch64",
            "device_classification": "high",
            "device_locale": "fr_FR",
            "device_manufacturer": "Pierre",
            "device_model": "ThePierrePhone",
            "device_os_build_number": "13",
            "device_os_name": "PierreOS",
            "device_os_version": "47",
            "duration_ns": 50000000000,
            "environment": "production",
            "organization_id": 1,
            "platform": "python",
            "profile_id": "a6cd859435584c3391412390168dcb93",
            "project_id": 1,
            "received": 1694357860,
            "retention_days": 30,
            "trace_id": "40300eb2e77c46908de27f4603befa45",
            "transaction_id": "b716a5ee27db49dcbb534dcca61a9df8",
            "transaction_name": "lets-get-ready-to-party",
            "version_code": "1337",
            "version_name": "v42.0.0"
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
        run_schema_type_test::<InputMessage>("processed-profiles", None);
    }
}
