use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use schemars::JsonSchema;
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

    let mut row = Chunk {
        chunk: msg,
        offset: metadata.offset,
        partition: metadata.partition,
    };

    row.chunk.retention_days = Some(enforce_retention(
        row.chunk.retention_days,
        &config.env_config,
    ));

    let origin_timestamp = DateTime::from_timestamp(row.chunk.received, 0);

    InsertBatch::from_rows([row], origin_timestamp)
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct InputMessage {
    project_id: u64,
    profiler_id: Uuid,
    chunk_id: Uuid,
    start_timestamp: f64,
    end_timestamp: f64,
    received: i64,
    retention_days: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct Chunk {
    #[serde(flatten)]
    chunk: InputMessage,

    #[serde(default)]
    offset: u64,
    #[serde(default)]
    partition: u16,
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
        run_schema_type_test::<InputMessage>("snuba-profile-chunks", None);
    }
}
