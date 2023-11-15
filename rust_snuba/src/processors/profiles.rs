use crate::types::{BadMessage, BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use uuid::Uuid;

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
) -> Result<BytesInsertBatch, BadMessage> {
    let payload_bytes = payload.payload.ok_or(BadMessage)?;
    let msg: FromProfileMessage = serde_json::from_slice(&payload_bytes).map_err(|err| {
        log::error!("Failed to deserialize message: {}", err);
        BadMessage
    })?;
    let mut profile_msg: ProfileMessage = msg.try_into()?;

    profile_msg.offset = metadata.offset;
    profile_msg.partition = metadata.partition;

    let serialized = serde_json::to_vec(&profile_msg).map_err(|err| {
        log::error!("Failed to serialize message: {}", err);
        BadMessage
    })?;

    Ok(BytesInsertBatch {
        rows: vec![serialized],
    })
}

#[derive(Debug, Deserialize)]
struct FromProfileMessage {
    #[serde(default)]
    android_api_level: Option<u32>,
    #[serde(default)]
    architecture: Option<String>,
    #[serde(default)]
    device_classification: Option<String>,
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
    profile_id: String,
    project_id: u64,
    received: i64,
    retention_days: u32,
    trace_id: String,
    transaction_id: String,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

#[derive(Default, Debug, Serialize)]
struct ProfileMessage {
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
    offset: u64,
    organization_id: u64,
    partition: u16,
    platform: String,
    profile_id: String,
    project_id: u64,
    received: i64,
    retention_days: u32,
    trace_id: String,
    transaction_id: String,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

impl TryFrom<FromProfileMessage> for ProfileMessage {
    type Error = BadMessage;
    fn try_from(from: FromProfileMessage) -> Result<ProfileMessage, BadMessage> {
        let profile_id = Uuid::parse_str(from.profile_id.as_str()).map_err(|_err| BadMessage)?;
        let trace_id = Uuid::parse_str(from.trace_id.as_str()).map_err(|_err| BadMessage)?;
        let transaction_id =
            Uuid::parse_str(from.transaction_id.as_str()).map_err(|_err| BadMessage)?;
        Ok(Self {
            android_api_level: from.android_api_level,
            architecture: from.architecture,
            device_classification: from.device_classification.unwrap_or_default(),
            device_locale: from.device_locale,
            device_manufacturer: from.device_manufacturer,
            device_model: from.device_model,
            device_os_build_number: from.device_os_build_number,
            device_os_name: from.device_os_name,
            device_os_version: from.device_os_version,
            duration_ns: from.duration_ns,
            environment: from.environment,
            organization_id: from.organization_id,
            platform: from.platform,
            profile_id: profile_id.to_string(),
            project_id: from.project_id,
            received: from.received,
            retention_days: from.retention_days,
            trace_id: trace_id.to_string(),
            transaction_id: transaction_id.to_string(),
            transaction_name: from.transaction_name,
            version_code: from.version_code,
            version_name: from.version_name,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

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
