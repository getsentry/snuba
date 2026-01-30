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
    let msg: FromChunkMessage = serde_json::from_slice(payload_bytes)?;
    let origin_timestamp = DateTime::from_timestamp(msg.received, 0);
    let mut chunk: Chunk = msg.into();

    chunk.offset = metadata.offset;
    chunk.partition = metadata.partition;
    chunk.retention_days = Some(enforce_retention(chunk.retention_days, &config.env_config));

    InsertBatch::from_rows([chunk], origin_timestamp)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FromChunkMessage {
    project_id: u64,
    profiler_id: Uuid,
    chunk_id: Uuid,
    start_timestamp: f64,
    end_timestamp: f64,
    environment: Option<String>,
    received: i64,
    retention_days: Option<u16>,
}

#[derive(Debug, Default, Serialize, JsonSchema, clickhouse::Row)]
struct Chunk {
    project_id: u64,
    profiler_id: Uuid,
    chunk_id: Uuid,
    #[serde(rename(serialize = "start_timestamp"))]
    start_timestamp_micro: u64,
    #[serde(rename(serialize = "end_timestamp"))]
    end_timestamp_micro: u64,
    environment: Option<String>,
    retention_days: Option<u16>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    partition: u16,
}

impl From<FromChunkMessage> for Chunk {
    fn from(from: FromChunkMessage) -> Chunk {
        Self {
            chunk_id: from.chunk_id,
            end_timestamp_micro: (from.end_timestamp * 1e6) as u64,
            profiler_id: from.profiler_id,
            project_id: from.project_id,
            environment: from.environment,
            retention_days: from.retention_days,
            start_timestamp_micro: (from.start_timestamp * 1e6) as u64,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::processors::tests::run_schema_type_test;

    use super::*;

    #[test]
    fn test_chunk() {
        let data = r#"{
            "chunk_id": "0432a0a4c25f4697bf9f0a2fcbe6a814",
            "end_timestamp": 1710805689.1234567,
            "profiler_id": "4d229f1d3807421ba62a5f8bc295d836",
            "project_id": 1,
            "received": 1694357860,
            "retention_days": 30,
            "start_timestamp": 1710805688.1234567
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
        run_schema_type_test::<FromChunkMessage>("snuba-profile-chunks", None);
    }
}
