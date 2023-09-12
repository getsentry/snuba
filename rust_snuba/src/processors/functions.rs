use crate::types::{BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::InvalidMessage;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> Result<BytesInsertBatch, InvalidMessage> {
    if let Some(payload_bytes) = payload.payload {
        let msg: FromFunctionsMessage = serde_json::from_slice(&payload_bytes).map_err(|err| {
            log::error!("Failed to deserialize message: {}", err);
            InvalidMessage
        })?;

        let profile_id = Uuid::parse_str(msg.profile_id.as_str()).map_err(|_err| InvalidMessage)?;
        let timestamp = match msg.timestamp {
            Some(timestamp) => timestamp,
            _ => SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_err| InvalidMessage)?.as_secs(),
        };
        let device_classification = match msg.device_class {
            Some(device_classification) => device_classification,
            _ => 0,
        };
        let mut rows = Vec::with_capacity(msg.functions.len());

        for from in &msg.functions {
            let function = Function{
                // Profile metadata
                browser_name: msg.browser_name.clone(),
                device_classification,
                dist: msg.dist.clone(),
                environment: msg.environment.clone(),
                http_method: msg.http_method.clone(),
                platform: msg.platform.clone(),
                profile_id: profile_id.to_string(),
                project_id: msg.project_id,
                release: msg.release.clone(),
                retention_days: msg.retention_days,
                timestamp,
                transaction_name: msg.transaction_name.clone(),
                transaction_op: msg.transaction_op.clone(),
                transaction_status: msg.transaction_status as u8,

                // Function metadata
                fingerprint: from.fingerprint,
                durations: from.self_times_ns.clone(),
                function: from.function.clone(),
                package: from.package.clone(),
                name: from.function.clone(),
                is_application: from.in_app as u8,

                ..Default::default()
            };
            let serialized = serde_json::to_vec(&function).map_err(|err| {
                log::error!("Failed to serialize message: {}", err);
                InvalidMessage
            })?;
            rows.push(serialized);
        }

        return Ok(BytesInsertBatch{rows});
    }
    Err(InvalidMessage)
}

#[derive(Debug, Deserialize)]
struct FromFunction {
    fingerprint: u64,
    function: String,
    in_app: bool,
    package: String,
    self_times_ns: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct FromFunctionsMessage {
    #[serde(default)]
    browser_name: Option<String>,
    #[serde(default)]
    device_class: Option<u32>,
    #[serde(default)]
    dist: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    functions: Vec<FromFunction>,
    #[serde(default)]
    http_method: Option<String>,
    platform: String,
    profile_id: String,
    project_id: u64,
    #[serde(default)]
    release: Option<String>,
    retention_days: u32,
    #[serde(default)]
    timestamp: Option<u64>,
    transaction_name: String,
    transaction_op: String,
    transaction_status: SpanStatus,
}

#[derive(Default, Debug, Serialize)]
struct Function {
    browser_name: Option<String>,
    device_classification: u32,
    dist: Option<String>,
    durations: Vec<u64>,
    environment: Option<String>,
    fingerprint: u64,
    function: String,
    http_method: Option<String>,
    is_application: u8,
    materialization_version: u8,
    module: String,
    name: String,
    package: String,
    platform: String,
    profile_id: String,
    project_id: u64,
    release: Option<String>,
    retention_days: u32,
    timestamp: u64,
    transaction_name: String,
    transaction_op: String,
    transaction_status: u8,

    // Deprecated fields
    depth: u8,
    os_name: String,
    os_version: String,
    parent_fingerprint: u8,
    path: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

    #[test]
    fn test_functions() {
        let data = r#"{
            "project_id": 22,
            "profile_id": "7329158c39964fbb9ec57c20cf4a2bb8",
            "transaction_name": "vroom-vroom",
            "timestamp": 1694447692,
            "functions": [
                {
                    "fingerprint": 123,
                    "function": "foo",
                    "package": "bar",
                    "in_app": true,
                    "self_times_ns": [1, 2, 3]
                },
                {
                    "fingerprint": 456,
                    "function": "baz",
                    "package": "qux",
                    "in_app": false,
                    "self_times_ns": [4, 5, 6]
                }
            ],
            "platform": "python",
            "environment": "prod",
            "release": "foo@1.0.0",
            "dist": "1",
            "transaction_op": "http.server",
            "transaction_status": "ok",
            "http_method": "GET",
            "browser_name": "Chrome",
            "device_class": 2,
            "retention_days": 30
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
