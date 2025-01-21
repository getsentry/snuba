use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let (rows, origin_timestamp) = deserialize_message(payload_bytes)?;

    InsertBatch::from_rows(rows, DateTime::from_timestamp(origin_timestamp as i64, 0))
}

pub fn deserialize_message(payload: &[u8]) -> anyhow::Result<(Vec<UptimeMonitorCheckRow>, f64)> {
    let monitor_message: UptimeMonitorCheckMessage = serde_json::from_slice(payload)?;

    let rows = vec![UptimeMonitorCheckRow {
        organization_id: monitor_message.organization_id,
        project_id: monitor_message.project_id,
        environment: monitor_message.environment,
        uptime_subscription_id: monitor_message.subscription_id,
        uptime_check_id: monitor_message.guid,
        scheduled_check_time: monitor_message.scheduled_check_time_ms,
        timestamp: monitor_message.actual_check_time_ms,
        duration_ms: monitor_message.duration_ms.unwrap_or(0),
        region: monitor_message.region.unwrap_or_default(),
        check_status: monitor_message.status,
        check_status_reason: monitor_message
            .status_reason
            .map(|r| r.ty)
            .unwrap_or_default(),
        http_status_code: monitor_message
            .request_info
            .unwrap_or_default()
            .http_status_code,
        trace_id: monitor_message.trace_id,
        retention_days: monitor_message.retention_days,
    }];
    Ok((rows, monitor_message.actual_check_time_ms as f64))
}

#[derive(Debug, Deserialize)]
struct UptimeMonitorCheckMessage<'a> {
    // TODO: add these to the message
    organization_id: u64,
    project_id: u64,
    retention_days: u16,
    region: Option<&'a str>,
    environment: Option<&'a str>,
    subscription_id: Uuid,
    guid: Uuid,
    scheduled_check_time_ms: u64,
    actual_check_time_ms: u64,
    duration_ms: Option<u64>,
    status: &'a str,
    status_reason: Option<CheckStatusReason<'a>>,
    trace_id: Uuid,
    request_info: Option<RequestInfo>,
}
#[derive(Debug, Deserialize, Default)]
pub struct RequestInfo {
    pub http_status_code: Option<u16>,
}
#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct CheckStatusReason<'a> {
    /// The type of the status reason
    #[serde(rename = "type")]
    pub ty: &'a str,

    /// A human readable description of the status reason
    pub description: String,
}

#[derive(Debug, Default, Serialize)]
pub struct UptimeMonitorCheckRow<'a> {
    organization_id: u64,
    project_id: u64,
    environment: Option<&'a str>,
    uptime_subscription_id: Uuid,
    uptime_check_id: Uuid,
    scheduled_check_time: u64,
    timestamp: u64,
    duration_ms: u64,
    region: &'a str,
    check_status: &'a str,
    check_status_reason: &'a str,
    http_status_code: Option<u16>,
    trace_id: Uuid,
    retention_days: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_monitor_checkin() {
        let data = r#"{
            "organization_id": 1,
            "project_id": 1,
            "retention_days": 30,
            "region": "global",
            "environment": "prod",
            "subscription_id": "123e4567-e89b-12d3-a456-426614174000",
            "guid": "550e8400-e29b-41d4-a716-446655440000",
            "scheduled_check_time_ms": 1702659277,
            "actual_check_time_ms": 1702659277,
            "duration_ms": 100,
            "status": "ok",
            "status_reason": {
                "type": "Request successful",
                "description": "Request successful"
            },
            "http_status_code": 200,
            "trace_id": "550e8400-e29b-41d4-a716-446655440000",
            "request_info": {
                "request_type": "GET",
                "http_status_code": 200
            }
        }"#;

        let (rows, timestamp) = deserialize_message(data.as_bytes()).unwrap();
        let monitor_row = rows.first().unwrap();

        assert_eq!(monitor_row.organization_id, 1);
        assert_eq!(monitor_row.project_id, 1);
        assert_eq!(monitor_row.environment, Some("prod"));
        assert_eq!(
            monitor_row.uptime_subscription_id,
            Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap()
        );
        assert_eq!(monitor_row.duration_ms, 100);
        assert_eq!(monitor_row.timestamp, 1702659277);
        assert_eq!(monitor_row.region, "global".to_string());
        assert_eq!(monitor_row.check_status, "ok");
        assert_eq!(monitor_row.check_status_reason, "Request successful");
        assert_eq!(monitor_row.http_status_code, Some(200));
        assert_eq!(monitor_row.retention_days, 30);
        assert_eq!(timestamp, 1702659277.0);
    }

    #[test]
    fn test_parse_monitor_checkin_null_duration() {
        let data = r#"{
            "organization_id": 1,
            "project_id": 1,
            "retention_days": 30,
            "region": "global",
            "environment": "prod",
            "subscription_id": "123e4567-e89b-12d3-a456-426614174000",
            "guid": "550e8400-e29b-41d4-a716-446655440000",
            "scheduled_check_time_ms": 1702659277,
            "actual_check_time_ms": 1702659277,
            "duration_ms": null,
            "status": "missed_window",
            "status_reason": null,
            "http_status_code": 200,
            "trace_id": "550e8400-e29b-41d4-a716-446655440000",
            "request_info": {
                "request_type": "GET",
                "http_status_code": 200
            }
        }"#;

        let (rows, timestamp) = deserialize_message(data.as_bytes()).unwrap();
        let monitor_row = rows.first().unwrap();

        assert_eq!(monitor_row.organization_id, 1);
        assert_eq!(monitor_row.project_id, 1);
        assert_eq!(monitor_row.environment, Some("prod"));
        assert_eq!(
            monitor_row.uptime_subscription_id,
            Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap()
        );
        assert_eq!(monitor_row.duration_ms, 0); // Duration should default to 0 when null
        assert_eq!(monitor_row.timestamp, 1702659277);
        assert_eq!(monitor_row.region, "global".to_string());
        assert_eq!(monitor_row.check_status, "missed_window");
        assert_eq!(monitor_row.check_status_reason, "");
        assert_eq!(monitor_row.http_status_code, Some(200));
        assert_eq!(monitor_row.retention_days, 30);
        assert_eq!(timestamp, 1702659277.0);
    }
}
