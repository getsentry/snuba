use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};

use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let (rows, origin_timestamp) =
        deserialize_message(payload_bytes, metadata.partition, metadata.offset)?;

    InsertBatch::from_rows(rows, DateTime::from_timestamp(origin_timestamp as i64, 0))
}

pub fn deserialize_message(
    payload: &[u8],
    partition: u16,
    offset: u64,
) -> anyhow::Result<(Vec<MonitorCheckInRow>, f64)> {
    let monitor_message: MonitorMessage = serde_json::from_slice(payload)?;

    let rows = vec![MonitorCheckInRow {
        monitor_id: monitor_message.monitor_id,
        project_id: monitor_message.project_id,
        organization_id: monitor_message.organization_id,
        status: monitor_message.status,
        duration_ms: monitor_message.duration_ms,
        timestamp: monitor_message.timestamp as u32,
        partition,
        offset,
    }];

    Ok((rows, monitor_message.start_time))
}

#[derive(Debug, Deserialize)]
struct MonitorMessage {
    monitor_id: u64,
    project_id: u64,
    organization_id: u64,
    status: String,
    duration_ms: u32,
    timestamp: f64,
    start_time: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct MonitorCheckInRow {
    monitor_id: u64,
    project_id: u64,
    organization_id: u64,
    status: String,
    duration_ms: u32,
    timestamp: u32,
    partition: u16,
    offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use sentry_arroyo::backends::kafka::types::KafkaPayload;

    #[test]
    fn test_parse_monitor_checkin() {
        let data = r#"{
            "monitor_id": 123,
            "project_id": 456,
            "organization_id": 789,
            "status": "ok",
            "duration_ms": 100,
            "timestamp": 1702659277,
            "start_time": 100
        }"#;

        let (rows, _) = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let monitor_row = rows.first().unwrap();

        assert_eq!(monitor_row.monitor_id, 123);
        assert_eq!(monitor_row.project_id, 456);
        assert_eq!(monitor_row.organization_id, 789);
        assert_eq!(&monitor_row.status, "ok");
        assert_eq!(monitor_row.duration_ms, 100);
        assert_eq!(monitor_row.timestamp, 1702659277);
        assert_eq!(monitor_row.partition, 0);
        assert_eq!(monitor_row.offset, 0);
    }
}
