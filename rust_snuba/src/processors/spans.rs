use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::Context;
use chrono::DateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use rust_arroyo::backends::kafka::types::KafkaPayload;

use crate::config::ProcessorConfig;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, InsertOrReplacement, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertOrReplacement<InsertBatch>> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromSpanMessage = serde_json::from_slice(payload_bytes)?;

    let origin_timestamp = DateTime::from_timestamp(msg.received as i64, 0);
    let mut span: Span = msg.try_into()?;

    span.retention_days = Some(enforce_retention(span.retention_days, &config.env_config));
    span.offset = metadata.offset;
    span.partition = metadata.partition;

    InsertBatch::from_rows([span], origin_timestamp)
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FromSpanMessage {
    #[serde(default)]
    _metrics_summary: Value,
    description: Option<String>,
    duration_ms: u32,
    event_id: Option<Uuid>,
    exclusive_time_ms: f64,
    is_segment: bool,
    measurements: Option<BTreeMap<String, FromMeasurementValue>>,
    parent_span_id: Option<String>,
    profile_id: Option<Uuid>,
    project_id: u64,
    received: f64,
    retention_days: Option<u16>,
    segment_id: Option<String>,
    sentry_tags: Option<BTreeMap<String, String>>,
    span_id: String,
    start_timestamp_ms: u64,
    tags: Option<BTreeMap<String, String>>,
    trace_id: Uuid,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FromMeasurementValue {
    value: f64,
}

#[derive(Debug, Default, Serialize)]
struct Span {
    #[serde(default)]
    action: String,
    deleted: u8,
    description: String,
    #[serde(default)]
    domain: String,
    duration: u32,
    end_ms: u16,
    end_timestamp: u64,
    exclusive_time: f64,
    group: u64,
    #[serde(default)]
    group_raw: u64,
    is_segment: u8,
    #[serde(default)]
    module: String,
    #[serde(rename(serialize = "measurements.key"))]
    measurement_keys: Vec<String>,
    #[serde(rename(serialize = "measurements.value"))]
    measurement_values: Vec<f64>,
    #[serde(default)]
    metrics_summary: String,
    offset: u64,
    op: String,
    parent_span_id: u64,
    partition: u16,
    platform: String,
    profile_id: Option<Uuid>,
    project_id: u64,
    retention_days: Option<u16>,
    segment_id: u64,
    segment_name: String,
    #[serde(rename(serialize = "sentry_tags.key"))]
    sentry_tag_keys: Vec<String>,
    #[serde(rename(serialize = "sentry_tags.value"))]
    sentry_tag_values: Vec<String>,
    span_id: u64,
    span_kind: String,
    span_status: u8,
    start_ms: u16,
    start_timestamp: u64,
    status: u32,
    #[serde(rename(serialize = "tags.key"))]
    tag_keys: Vec<String>,
    #[serde(rename(serialize = "tags.value"))]
    tag_values: Vec<String>,
    trace_id: Uuid,
    transaction_id: Option<Uuid>,
    transaction_op: String,
    user: String,
}

impl TryFrom<FromSpanMessage> for Span {
    type Error = anyhow::Error;

    fn try_from(from: FromSpanMessage) -> anyhow::Result<Span> {
        let end_timestamp_ms = from.start_timestamp_ms + from.duration_ms as u64;
        let sentry_tags = from.sentry_tags.unwrap_or_default();
        let group: u64 = sentry_tags
            .get("group")
            .map(|group| u64::from_str_radix(group, 16).unwrap_or_default())
            .unwrap_or_default();
        let status = sentry_tags
            .get("status")
            .map(|status| status.as_str())
            .map_or(SpanStatus::Unknown, |status| {
                SpanStatus::from_str(status).unwrap_or_default()
            });
        let transaction_op = sentry_tags
            .get("transaction.op")
            .cloned()
            .unwrap_or("".to_string());

        let (sentry_tag_keys, sentry_tag_values) = sentry_tags.clone().into_iter().unzip();
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) =
            from.tags.unwrap_or_default().into_iter().unzip();

        let (measurement_keys, measurement_values) = from
            .measurements
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.value))
            .unzip();

        let metrics_summary = match from._metrics_summary {
            Value::Object(v) => serde_json::to_string(&v).unwrap_or_default(),
            _ => "".into(),
        };

        Ok(Self {
            action: sentry_tags.get("action").cloned().unwrap_or_default(),
            description: from.description.unwrap_or_default(),
            domain: sentry_tags.get("domain").cloned().unwrap_or_default(),
            duration: from.duration_ms,
            end_ms: (end_timestamp_ms % 1000) as u16,
            end_timestamp: end_timestamp_ms / 1000,
            exclusive_time: from.exclusive_time_ms,
            group,
            is_segment: if from.is_segment { 1 } else { 0 },
            measurement_keys,
            measurement_values,
            metrics_summary,
            module: sentry_tags.get("module").cloned().unwrap_or_default(),
            op: sentry_tags.get("op").cloned().unwrap_or_default(),
            parent_span_id: from.parent_span_id.map_or(0, |parent_span_id| {
                u64::from_str_radix(&parent_span_id, 16).unwrap_or_default()
            }),
            platform: sentry_tags.get("system").cloned().unwrap_or_default(),
            profile_id: from.profile_id,
            project_id: from.project_id,
            retention_days: from.retention_days,
            segment_id: from.segment_id.map_or(0, |segment_id| {
                u64::from_str_radix(&segment_id, 16).unwrap_or_default()
            }),
            segment_name: sentry_tags.get("transaction").cloned().unwrap_or_default(),
            sentry_tag_keys,
            sentry_tag_values,
            span_id: u64::from_str_radix(&from.span_id, 16)?,
            span_status: status as u8,
            start_ms: (from.start_timestamp_ms % 1000) as u16,
            start_timestamp: from.start_timestamp_ms / 1000,
            status: status as u32,
            tag_keys,
            tag_values,
            trace_id: from.trace_id,
            transaction_id: from.event_id,
            transaction_op,
            user: sentry_tags.get("user").cloned().unwrap_or_default(),
            ..Default::default()
        })
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[repr(u8)] // size limit in clickhouse
pub enum SpanStatus {
    /// The operation completed successfully.
    ///
    /// HTTP status 100..299 + successful redirects from the 3xx range.
    Ok = 0,

    /// The operation was cancelled (typically by the user).
    Cancelled = 1,

    /// Unknown. Any non-standard HTTP status code.
    ///
    /// "We do not know whether the transaction failed or succeeded"
    #[default]
    #[serde(alias = "")]
    Unknown = 2,

    /// Client specified an invalid argument. 4xx.
    ///
    /// Note that this differs from FailedPrecondition. InvalidArgument indicates arguments that
    /// are problematic regardless of the state of the system.
    InvalidArgument = 3,

    /// Deadline expired before operation could complete.
    ///
    /// For operations that change the state of the system, this error may be returned even if the
    /// operation has been completed successfully.
    ///
    /// HTTP redirect loops and 504 Gateway Timeout
    DeadlineExceeded = 4,

    /// 404 Not Found. Some requested entity (file or directory) was not found.
    NotFound = 5,

    /// Already exists (409)
    ///
    /// Some entity that we attempted to create already exists.
    AlreadyExists = 6,

    /// 403 Forbidden
    ///
    /// The caller does not have permission to execute the specified operation.
    PermissionDenied = 7,

    /// 429 Too Many Requests
    ///
    /// Some resource has been exhausted, perhaps a per-user quota or perhaps the entire file
    /// system is out of space.
    ResourceExhausted = 8,

    /// Operation was rejected because the system is not in a state required for the operation's
    /// execution
    FailedPrecondition = 9,

    /// The operation was aborted, typically due to a concurrency issue.
    Aborted = 10,

    /// Operation was attempted past the valid range.
    OutOfRange = 11,

    /// 501 Not Implemented
    ///
    /// Operation is not implemented or not enabled.
    Unimplemented = 12,

    /// Other/generic 5xx.
    InternalError = 13,

    /// 503 Service Unavailable
    Unavailable = 14,

    /// Unrecoverable data loss or corruption
    DataLoss = 15,

    /// 401 Unauthorized (actually does mean unauthenticated according to RFC 7235)
    ///
    /// Prefer PermissionDenied if a user is logged in.
    Unauthenticated = 16,
}

pub struct ParseSpanStatusError;

impl FromStr for SpanStatus {
    type Err = ParseSpanStatusError;

    fn from_str(string: &str) -> Result<SpanStatus, Self::Err> {
        Ok(match string {
            "ok" => SpanStatus::Ok,
            "success" => SpanStatus::Ok, // Backwards compat with initial schema
            "deadline_exceeded" => SpanStatus::DeadlineExceeded,
            "unauthenticated" => SpanStatus::Unauthenticated,
            "permission_denied" => SpanStatus::PermissionDenied,
            "not_found" => SpanStatus::NotFound,
            "resource_exhausted" => SpanStatus::ResourceExhausted,
            "invalid_argument" => SpanStatus::InvalidArgument,
            "unimplemented" => SpanStatus::Unimplemented,
            "unavailable" => SpanStatus::Unavailable,
            "internal_error" => SpanStatus::InternalError,
            "failure" => SpanStatus::InternalError, // Backwards compat with initial schema
            "unknown" | "unknown_error" => SpanStatus::Unknown,
            "cancelled" => SpanStatus::Cancelled,
            "already_exists" => SpanStatus::AlreadyExists,
            "failed_precondition" => SpanStatus::FailedPrecondition,
            "aborted" => SpanStatus::Aborted,
            "out_of_range" => SpanStatus::OutOfRange,
            "data_loss" => SpanStatus::DataLoss,
            _ => return Err(ParseSpanStatusError),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::processors::tests::run_schema_type_test;

    use super::*;

    #[derive(Debug, Default, Deserialize, Serialize)]
    struct TestSentryTags {
        action: Option<String>,
        domain: Option<String>,
        group: Option<String>,
        #[serde(rename = "http.method")]
        http_method: Option<String>,
        module: Option<String>,
        op: Option<String>,
        status: Option<String>,
        status_code: Option<String>,
        system: Option<String>,
        transaction: Option<String>,
        #[serde(rename = "transaction.method")]
        transaction_method: Option<String>,
        #[serde(rename = "transaction.op")]
        transaction_op: Option<String>,
    }

    #[derive(Debug, Default, Deserialize, Serialize)]
    struct TestSpanMessage {
        description: Option<String>,
        duration_ms: Option<u32>,
        event_id: Option<Uuid>,
        exclusive_time_ms: Option<f64>,
        is_segment: Option<bool>,
        parent_span_id: Option<String>,
        profile_id: Option<Uuid>,
        project_id: Option<u64>,
        received: Option<f64>,
        retention_days: Option<u16>,
        segment_id: Option<String>,
        sentry_tags: TestSentryTags,
        span_id: Option<String>,
        start_timestamp_ms: Option<u64>,
        tags: Option<BTreeMap<String, String>>,
        trace_id: Option<Uuid>,
    }

    fn valid_span() -> TestSpanMessage {
        TestSpanMessage {
            description: Some("GET /blah".into()),
            duration_ms: Some(1000),
            event_id: Some(Uuid::new_v4()),
            exclusive_time_ms: Some(1000.0),
            is_segment: Some(false),
            parent_span_id: Some("deadbeefdeadbeef".into()),
            profile_id: Some(Uuid::new_v4()),
            project_id: Some(1),
            retention_days: Some(90),
            received: Some(1691105878.720),
            segment_id: Some("deadbeefdeadbeef".into()),
            sentry_tags: TestSentryTags {
                action: Some("GET".into()),
                domain: Some("targetdomain.tld:targetport".into()),
                group: Some("deadbeefdeadbeef".into()),
                http_method: Some("GET".into()),
                module: Some("http".into()),
                op: Some("http.client".into()),
                status: Some("ok".into()),
                status_code: Some("200".into()),
                system: Some("python".into()),
                transaction: Some("/organizations/:orgId/issues/".into()),
                transaction_method: Some("GET".into()),
                transaction_op: Some("navigation".into()),
            },
            span_id: Some("deadbeefdeadbeef".into()),
            start_timestamp_ms: Some(1691105878720),
            tags: Some(BTreeMap::from([
                ("tag1".into(), "value1".into()),
                ("tag2".into(), "123".into()),
                ("tag3".into(), "true".into()),
            ])),
            trace_id: Some(Uuid::new_v4()),
        }
    }

    #[test]
    fn test_valid_span() {
        let span = valid_span();
        let data = serde_json::to_string(&span);
        assert!(data.is_ok());
        let payload = KafkaPayload::new(None, None, Some(data.unwrap().as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn test_empty_status_value() {
        let mut span = valid_span();
        span.sentry_tags.status = Some("".into());
        let data = serde_json::to_vec(&span).unwrap();
        let payload = KafkaPayload::new(None, None, Some(data));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn test_null_retention_days() {
        let mut span = valid_span();
        span.retention_days = None;
        let data = serde_json::to_vec(&span).unwrap();
        let payload = KafkaPayload::new(None, None, Some(data));

        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn test_null_tags() {
        let mut span = valid_span();
        span.tags = Option::None;
        let data = serde_json::to_vec(&span).unwrap();
        let payload = KafkaPayload::new(None, None, Some(data));
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
        run_schema_type_test::<FromSpanMessage>("snuba-spans");
    }
}
