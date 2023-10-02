use crate::types::{BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::InvalidMessage;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> Result<BytesInsertBatch, InvalidMessage> {
    if let Some(payload_bytes) = payload.payload {
        let msg: FromSpanMessage = serde_json::from_slice(&payload_bytes).map_err(|err| {
            log::error!("Failed to deserialize message: {}", err);
            InvalidMessage
        })?;
        let mut span: Span = msg.try_into()?;

        span.offset = _metadata.offset;
        span.partition = _metadata.partition;

        let serialized = serde_json::to_vec(&span).map_err(|err| {
            log::error!("Failed to serialize processed message: {}", err);
            InvalidMessage
        })?;

        return Ok(BytesInsertBatch {
            rows: vec![serialized],
        });
    }
    Err(InvalidMessage)
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
    project_id: u64,
    retention_days: u16,
    #[serde(deserialize_with = "hex_to_u64")]
    segment_id: u64,
    sentry_tags: FromSentryTags,
    #[serde(deserialize_with = "hex_to_u64")]
    span_id: u64,
    start_timestamp_ms: u64,
    tags: BTreeMap<String, String>,
    trace_id: Uuid,
}

#[derive(Debug, Default, Deserialize)]
struct FromSentryTags {
    #[serde(default)]
    action: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    group: String,
    #[serde(rename(deserialize = "http.method"))]
    http_method: Option<String>,
    #[serde(default)]
    module: String,
    #[serde(default)]
    op: String,
    #[serde(default)]
    status: SpanStatus,
    status_code: Option<String>,
    #[serde(default)]
    system: String,
    #[serde(default)]
    transaction: String,
    #[serde(rename(deserialize = "transaction.method"))]
    transaction_method: Option<String>,
    #[serde(default, rename(deserialize = "transaction.op"))]
    transaction_op: String,
}

impl FromSentryTags {
    fn to_keys_values(&self) -> (Vec<String>, Vec<String>) {
        let mut tags: BTreeMap<String, String> = BTreeMap::new();

        tags.insert("action".into(), self.action.clone());
        tags.insert("domain".into(), self.domain.clone());
        tags.insert("group".into(), self.group.clone());
        tags.insert("module".into(), self.module.clone());
        tags.insert("op".into(), self.op.clone());
        tags.insert("status".into(), self.status.as_str().to_string());
        tags.insert("system".into(), self.system.clone());
        tags.insert("transaction".into(), self.transaction.clone());
        tags.insert("transaction.op".into(), self.transaction_op.clone());

        if let Some(http_method) = &self.http_method {
            tags.insert("http.method".into(), http_method.into());
        }

        if let Some(transaction_method) = &self.transaction_method {
            tags.insert("transaction.method".into(), transaction_method.into());
        }

        if let Some(status_code) = &self.status_code {
            tags.insert("status_code".into(), status_code.into());
        }

        (
            tags.keys().cloned().collect(),
            tags.values().cloned().collect(),
        )
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
    type Error = InvalidMessage;

    fn try_from(from: FromSpanMessage) -> Result<Span, InvalidMessage> {
        let end_timestamp_ms = from.start_timestamp_ms + from.duration_ms as u64;
        let status = from.sentry_tags.status as u8;
        let transaction_op = from.sentry_tags.transaction_op.clone();
        let (sentry_tag_keys, sentry_tag_values) = from.sentry_tags.to_keys_values();
        let mut tag_keys: Vec<String> = from.tags.clone().into_keys().collect();
        let mut tag_values: Vec<String> = from.tags.into_values().collect();

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
            action: from.sentry_tags.action.clone(),
            description: from.description,
            domain: from.sentry_tags.domain.clone(),
            duration: from.duration_ms,
            end_ms: (end_timestamp_ms % 1000) as u16,
            end_timestamp: end_timestamp_ms / 1000,
            exclusive_time: from.exclusive_time_ms,
            group: u64::from_str_radix(&from.sentry_tags.group, 16).map_err(|_| InvalidMessage)?,
            group_raw: from.group_raw,
            is_segment: if from.is_segment { 1 } else { 0 },
            module: from.sentry_tags.module.clone(),
            op: from.sentry_tags.op.clone(),
            parent_span_id: from.parent_span_id,
            platform: from.sentry_tags.system.clone(),
            project_id: from.project_id,
            retention_days: from.retention_days,
            segment_id: from.segment_id,
            segment_name: from.sentry_tags.transaction.clone(),
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

fn hex_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let hex = String::deserialize(deserializer)?;
    u64::from_str_radix(&hex, 16).map_err(serde::de::Error::custom)
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
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

    #[test]
    fn test_valid_span() {
        let data = r#"{
            "duration_ms": 1000,
            "event_id": "dcc403b73ef548648188bbfa6012e9dc",
            "exclusive_time_ms": 1000,
            "group_raw": "b640a0ce465fa2a4",
            "is_segment": false,
            "organization_id": 69,
            "parent_span_id": "deadbeefdeadbeef",
            "project_id": 1,
            "retention_days": 90,
            "segment_id": "deadbeefdeadbeef",
            "span_id": "deadbeefdeadbeef",
            "start_timestamp_ms": 1691105878720,
            "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
            "tags": {
              "tag1": "value1",
              "tag2": "123",
              "tag3": "true"
            },
            "sentry_tags": {
              "action": "GET",
              "domain": "targetdomain.tld:targetport",
              "group": "deadbeefdeadbeef",
              "http.method": "GET",
              "module": "http",
              "op": "http.client",
              "status": "ok",
              "status_code": "200",
              "system": "python",
              "transaction": "/organizations/:orgId/issues/",
              "transaction.method": "GET",
              "transaction.op": "navigation"
            }
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

    #[test]
    fn test_no_status_value() {
        let data = r#"{
            "duration_ms": 1000,
            "event_id": "dcc403b73ef548648188bbfa6012e9dc",
            "exclusive_time_ms": 1000,
            "group_raw": "b640a0ce465fa2a4",
            "is_segment": false,
            "organization_id": 69,
            "parent_span_id": "deadbeefdeadbeef",
            "project_id": 1,
            "retention_days": 90,
            "segment_id": "deadbeefdeadbeef",
            "span_id": "deadbeefdeadbeef",
            "start_timestamp_ms": 1691105878720,
            "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
            "tags": {
              "tag1": "value1",
              "tag2": "123",
              "tag3": "true"
            },
            "sentry_tags": {
              "action": "GET",
              "domain": "targetdomain.tld:targetport",
              "group": "deadbeefdeadbeef",
              "http.method": "GET",
              "module": "http",
              "op": "http.client",
              "status_code": "200",
              "system": "python",
              "transaction": "/organizations/:orgId/issues/",
              "transaction.method": "GET",
              "transaction.op": "navigation"
            }
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

    #[test]
    fn test_empty_status_value() {
        let data = r#"{
            "duration_ms": 1000,
            "event_id": "dcc403b73ef548648188bbfa6012e9dc",
            "exclusive_time_ms": 1000,
            "group_raw": "b640a0ce465fa2a4",
            "is_segment": false,
            "organization_id": 69,
            "parent_span_id": "deadbeefdeadbeef",
            "project_id": 1,
            "retention_days": 90,
            "segment_id": "deadbeefdeadbeef",
            "span_id": "deadbeefdeadbeef",
            "start_timestamp_ms": 1691105878720,
            "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
            "tags": {
              "tag1": "value1",
              "tag2": "123",
              "tag3": "true"
            },
            "sentry_tags": {
              "action": "GET",
              "domain": "targetdomain.tld:targetport",
              "group": "deadbeefdeadbeef",
              "http.method": "GET",
              "module": "http",
              "op": "http.client",
              "status": "",
              "status_code": "200",
              "system": "python",
              "transaction": "/organizations/:orgId/issues/",
              "transaction.method": "GET",
              "transaction.op": "navigation"
            }
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
