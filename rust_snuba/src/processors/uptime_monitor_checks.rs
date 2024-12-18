use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
) -> anyhow::Result<(Vec<UptimeMonitorCheckRow>, f64)> {
    let monitor_message: UptimeMonitorCheckMessage = serde_json::from_slice(payload)?;

    let rows = vec![UptimeMonitorCheckRow {
        organization_id: monitor_message.organization_id,
        project_id: monitor_message.project_id,
        environment: monitor_message.environment,
        uptime_subscription_id: monitor_message.uptime_subscription_id,
        uptime_check_id: monitor_message.uptime_check_id,
        scheduled_check_time: monitor_message.scheduled_check_time as u32,
        timestamp: monitor_message.timestamp as u32,
        _sort_timestamp: monitor_message.timestamp as u32,
        duration: monitor_message.duration,
        region_id: monitor_message.region_id,
        check_status: monitor_message.check_status,
        check_status_reason: monitor_message.check_status_reason,
        http_status_code: monitor_message.http_status_code,
        trace_id: monitor_message.trace_id,
        retention_days: monitor_message.retention_days,
        partition,
        offset,
    }];

    Ok((rows, monitor_message.timestamp))
}

#[derive(Debug, Deserialize)]
struct UptimeMonitorCheckMessage {
    organization_id: u64,
    project_id: u64,
    environment: Option<String>,
    uptime_subscription_id: u64,
    uptime_check_id: Uuid,
    scheduled_check_time: f64,
    timestamp: f64,
    duration: u64,
    region_id: Option<u16>,
    check_status: String,
    check_status_reason: Option<String>,
    http_status_code: u16,
    trace_id: Uuid,
    retention_days: u16,
}

#[derive(Debug, Default, Serialize)]
pub struct UptimeMonitorCheckRow {
    organization_id: u64,
    project_id: u64,
    environment: Option<String>,
    uptime_subscription_id: u64,
    uptime_check_id: Uuid,
    scheduled_check_time: u32,
    timestamp: u32,
    _sort_timestamp: u32,
    duration: u64,
    region_id: Option<u16>,
    check_status: String,
    check_status_reason: Option<String>,
    http_status_code: u16,
    trace_id: Uuid,
    retention_days: u16,
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
            "organization_id": 789,
            "project_id": 456,
            "environment": "prod",
            "uptime_subscription_id": 123,
            "uptime_check_id": "550e8400-e29b-41d4-a716-446655440000",
            "scheduled_check_time": 1702659277,
            "timestamp": 1702659277,
            "duration": 100,
            "region_id": 42,
            "check_status": "ok",
            "check_status_reason": "Request successful",
            "http_status_code": 200,
            "trace_id": "550e8400-e29b-41d4-a716-446655440000",
            "retention_days": 30
        }"#;

        let (rows, timestamp) = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let monitor_row = rows.first().unwrap();

        assert_eq!(monitor_row.organization_id, 789);
        assert_eq!(monitor_row.project_id, 456);
        assert_eq!(monitor_row.environment, Some("prod".to_string()));
        assert_eq!(monitor_row.uptime_subscription_id, 123);
        assert_eq!(monitor_row.duration, 100);
        assert_eq!(monitor_row.timestamp, 1702659277);
        assert_eq!(monitor_row._sort_timestamp, 1702659277);
        assert_eq!(monitor_row.region_id, Some(42));
        assert_eq!(&monitor_row.check_status, "ok");
        assert_eq!(
            monitor_row.check_status_reason,
            Some("Request successful".to_string())
        );
        assert_eq!(monitor_row.http_status_code, 200);
        assert_eq!(monitor_row.retention_days, 30);
        assert_eq!(monitor_row.partition, 0);
        assert_eq!(monitor_row.offset, 0);
        assert_eq!(timestamp, 1702659277.0);
    }
}
