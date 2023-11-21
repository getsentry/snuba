use std::collections::BTreeMap;

use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processors::utils::{default_retention_days, hex_to_u64, DEFAULT_RETENTION_DAYS};
use crate::types::{BytesInsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
) -> anyhow::Result<BytesInsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromSpanMessage = serde_json::from_slice(payload_bytes)?;

    let mut span: Span = msg.try_into()?;

    span.offset = metadata.offset;
    span.partition = metadata.partition;

    let serialized = serde_json::to_vec(&span)?;

    Ok(BytesInsertBatch::from_rows(vec![serialized]))
}

#[derive(Debug, Default, Deserialize)]
struct FromSpanMessage {
    #[serde(default)]
    description: String,
    duration_ms: u32,
    event_id: Uuid,
    exclusive_time_ms: f64,
    #[serde(deserialize_with = "hex_to_u64")]
    group_raw: u64,
    is_segment: bool,
    #[serde(deserialize_with = "hex_to_u64")]
    parent_span_id: u64,
    profile_id: Option<Uuid>,
    project_id: u64,
    #[serde(default = "default_retention_days")]
    retention_days: Option<u16>,
    #[serde(deserialize_with = "hex_to_u64")]
    segment_id: u64,
    sentry_tags: FromSentryTags,
    #[serde(deserialize_with = "hex_to_u64")]
    span_id: u64,
    start_timestamp_ms: u64,
    tags: Option<BTreeMap<String, String>>,
    trace_id: Uuid,
}

#[derive(Debug, Default, Deserialize)]
struct FromSentryTags {
    action: Option<String>,
    domain: Option<String>,
    group: Option<String>,
    #[serde(rename(deserialize = "http.method"))]
    http_method: Option<String>,
    module: Option<String>,
    op: Option<String>,
    status: Option<SpanStatus>,
    status_code: Option<String>,
    system: Option<String>,
    transaction: Option<String>,
    #[serde(rename(deserialize = "transaction.method"))]
    transaction_method: Option<String>,
    #[serde(rename(deserialize = "transaction.op"))]
    transaction_op: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

impl FromSentryTags {
    fn to_keys_values(&self) -> (Vec<String>, Vec<String>) {
        let mut tags: BTreeMap<String, String> = BTreeMap::new();

        if let Some(action) = &self.action {
            tags.insert("action".into(), action.into());
        }

        if let Some(domain) = &self.domain {
            tags.insert("domain".into(), domain.into());
        }

        if let Some(group) = &self.group {
            tags.insert("group".into(), group.into());
        }

        if let Some(module) = &self.module {
            tags.insert("module".into(), module.into());
        }

        if let Some(op) = &self.op {
            tags.insert("op".into(), op.into());
        }

        if let Some(status) = &self.status {
            tags.insert("status".into(), status.as_str().to_string());
        }

        if let Some(system) = &self.system {
            tags.insert("system".into(), system.into());
        }

        if let Some(transaction) = &self.transaction {
            tags.insert("transaction".into(), transaction.into());
        }

        if let Some(transaction_op) = &self.transaction_op {
            tags.insert("transaction.op".into(), transaction_op.into());
        }

        if let Some(http_method) = &self.http_method {
            tags.insert("http.method".into(), http_method.into());
        }

        if let Some(transaction_method) = &self.transaction_method {
            tags.insert("transaction.method".into(), transaction_method.into());
        }

        if let Some(status_code) = &self.status_code {
            tags.insert("status_code".into(), status_code.into());
        }

        for (key, value) in &self.extra {
            tags.insert(key.into(), value.into());
        }

        tags.into_iter().unzip()
    }
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
    group_raw: u64,
    is_segment: u8,
    #[serde(default)]
    module: String,
    #[serde(rename(serialize = "measurements.key"))]
    measurement_keys: Vec<String>,
    #[serde(rename(serialize = "measurements.value"))]
    measurement_values: Vec<f64>,
    offset: u64,
    op: String,
    parent_span_id: u64,
    partition: u16,
    platform: String,
    profile_id: Option<Uuid>,
    project_id: u64,
    retention_days: u16,
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
    transaction_id: Uuid,
    transaction_op: String,
    user: String,
}

impl TryFrom<FromSpanMessage> for Span {
    type Error = anyhow::Error;

    fn try_from(from: FromSpanMessage) -> anyhow::Result<Span> {
        let end_timestamp_ms = from.start_timestamp_ms + from.duration_ms as u64;
        let group = if let Some(group) = &from.sentry_tags.group {
            u64::from_str_radix(group, 16)?
        } else {
            0
        };
        let status = from.sentry_tags.status.unwrap_or_default() as u8;
        let (sentry_tag_keys, sentry_tag_values) = from.sentry_tags.to_keys_values();
        let transaction_op = from.sentry_tags.transaction_op.unwrap_or_default();

        let tags = from.tags.unwrap_or_default();
        let (mut tag_keys, mut tag_values): (Vec<_>, Vec<_>) = tags.into_iter().unzip();

        if let Some(http_method) = from.sentry_tags.http_method.clone() {
            tag_keys.push("http.method".into());
            tag_values.push(http_method);
        }

        if let Some(status_code) = &from.sentry_tags.status_code {
            tag_keys.push("status_code".into());
            tag_values.push(status_code.into());
        }

        if let Some(transaction_method) = from.sentry_tags.transaction_method.clone() {
            tag_keys.push("transaction.method".into());
            tag_values.push(transaction_method);
        }

        Ok(Self {
            action: from.sentry_tags.action.unwrap_or_default(),
            description: from.description,
            domain: from.sentry_tags.domain.unwrap_or_default(),
            duration: from.duration_ms,
            end_ms: (end_timestamp_ms % 1000) as u16,
            end_timestamp: end_timestamp_ms / 1000,
            exclusive_time: from.exclusive_time_ms,
            group,
            group_raw: from.group_raw,
            is_segment: if from.is_segment { 1 } else { 0 },
            module: from.sentry_tags.module.unwrap_or_default(),
            op: from.sentry_tags.op.unwrap_or_default(),
            parent_span_id: from.parent_span_id,
            platform: from.sentry_tags.system.unwrap_or_default(),
            profile_id: from.profile_id,
            project_id: from.project_id,
            retention_days: from.retention_days.unwrap_or(DEFAULT_RETENTION_DAYS),
            segment_id: from.segment_id,
            segment_name: from.sentry_tags.transaction.unwrap_or_default(),
            sentry_tag_keys,
            sentry_tag_values,
            span_id: from.span_id,
            span_status: status,
            start_ms: (from.start_timestamp_ms % 1000) as u16,
            start_timestamp: from.start_timestamp_ms / 1000,
            status: status.into(),
            tag_keys,
            tag_values,
            trace_id: from.trace_id,
            transaction_id: from.event_id,
            transaction_op,
            ..Default::default()
        })
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, Serialize)]
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

impl SpanStatus {
    /// Returns the string representation of the status.
    pub fn as_str(&self) -> &'static str {
        match *self {
            SpanStatus::Ok => "ok",
            SpanStatus::DeadlineExceeded => "deadline_exceeded",
            SpanStatus::Unauthenticated => "unauthenticated",
            SpanStatus::PermissionDenied => "permission_denied",
            SpanStatus::NotFound => "not_found",
            SpanStatus::ResourceExhausted => "resource_exhausted",
            SpanStatus::InvalidArgument => "invalid_argument",
            SpanStatus::Unimplemented => "unimplemented",
            SpanStatus::Unavailable => "unavailable",
            SpanStatus::InternalError => "internal_error",
            SpanStatus::Unknown => "unknown",
            SpanStatus::Cancelled => "cancelled",
            SpanStatus::AlreadyExists => "already_exists",
            SpanStatus::FailedPrecondition => "failed_precondition",
            SpanStatus::Aborted => "aborted",
            SpanStatus::OutOfRange => "out_of_range",
            SpanStatus::DataLoss => "data_loss",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use std::time::SystemTime;

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
        group_raw: Option<String>,
        is_segment: Option<bool>,
        parent_span_id: Option<String>,
        profile_id: Option<Uuid>,
        project_id: Option<u64>,
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
            group_raw: Some("deadbeefdeadbeef".into()),
            is_segment: Some(false),
            parent_span_id: Some("deadbeefdeadbeef".into()),
            profile_id: Some(Uuid::new_v4()),
            project_id: Some(1),
            retention_days: Some(90),
            segment_id: Some("deadbeefdeadbeef".into()),
            span_id: Some("deadbeefdeadbeef".into()),
            start_timestamp_ms: Some(1691105878720),
            trace_id: Some(Uuid::new_v4()),
            tags: Some(BTreeMap::from([
                ("tag1".into(), "value1".into()),
                ("tag2".into(), "123".into()),
                ("tag3".into(), "true".into()),
            ])),
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
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_null_status_value() {
        let mut span = valid_span();
        span.sentry_tags.status = Option::None;
        let data = serde_json::to_string(&span);
        assert!(data.is_ok());
        let payload = KafkaPayload::new(None, None, Some(data.unwrap().as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_empty_status_value() {
        let mut span = valid_span();
        span.sentry_tags.status = Some("".into());
        let data = serde_json::to_string(&span);
        assert!(data.is_ok());
        let payload = KafkaPayload::new(None, None, Some(data.unwrap().as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_null_retention_days() {
        let mut span = valid_span();
        span.retention_days = default_retention_days();
        let data = serde_json::to_string(&span);
        assert!(data.is_ok());
        let payload = KafkaPayload::new(None, None, Some(data.unwrap().as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_null_tags() {
        let mut span = valid_span();
        span.tags = Option::None;
        let data = serde_json::to_string(&span);
        assert!(data.is_ok());
        let payload = KafkaPayload::new(None, None, Some(data.unwrap().as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
