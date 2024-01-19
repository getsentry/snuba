use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

use rust_arroyo::backends::kafka::types::KafkaPayload;

use crate::config::ProcessorConfig;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata, RowData};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: ErrorMessage = serde_json::from_slice(payload_bytes)?;
    let origin_timestamp = DateTime::from_timestamp(msg.data.received as i64, 0);

    let mut row: ErrorRow = msg.try_into()?;
    row.partition = metadata.partition;
    row.offset = metadata.offset;

    Ok(InsertBatch {
        origin_timestamp,
        rows: RowData::from_rows([row])?,
        sentry_received_timestamp: None,
    })
}

#[derive(Debug, Deserialize)]
struct ErrorMessage {
    data: ErrorData,
    datetime: String,
    event_id: Uuid,
    group_id: u64,
    platform: String,
    project_id: u64,
    retention_days: u16,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorData {
    contexts: Contexts,
    culprit: String,
    event_id: Uuid,
    hierarchical_hashes: Vec<String>,
    location: String,
    message: String,
    modules: HashMap<String, Option<String>>,
    platform: String,
    primary_hash: String,
    received: f64,
    request: Request,
    sdk: Sdk,
    tags: Vec<(String, Option<String>)>,
    title: String,
    ty: String,
    user: User,
    version: String,
}

// Contexts

#[derive(Debug, Deserialize)]
struct Contexts {
    #[serde(default)]
    replay: ReplayContext,
    #[serde(default)]
    trace: TraceContext,
}

#[derive(Debug, Default, Deserialize)]
struct TraceContext {
    #[serde(default)]
    sampled: Option<bool>,
    #[serde(default)]
    span_id: Option<u64>,
    #[serde(default)]
    trace_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ReplayContext {
    #[serde(default)]
    replay_id: Option<Uuid>,
}

// Stacktraces

#[derive(Debug, Deserialize)]
struct StrackTrace {
    #[serde(default)]
    frames: Option<Vec<StrackFrame>>,
    #[serde(default)]
    mechanism: Option<StackMechanism>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default, rename = "type")]
    ty: Option<String>,
    #[serde(default)]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StrackFrame {
    #[serde(default)]
    abs_path: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    package: Option<String>,
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    function: Option<String>,
    #[serde(default)]
    in_app: Option<bool>,
    #[serde(default)]
    colno: Option<u32>,
    #[serde(default)]
    lineno: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct StackMechanism {
    #[serde(default, rename = "type")]
    ty: Option<String>,
    #[serde(default)]
    handled: Option<Value>,
}

// SDK

#[derive(Debug, Deserialize)]
struct Sdk {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    integrations: Vec<String>,
}

// Request

#[derive(Debug, Deserialize)]
struct Request {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    headers: Vec<(String, Option<String>)>,
}

// User

#[derive(Debug, Deserialize)]
struct User {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    ip_address: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

// Row

#[derive(Debug, Default, Serialize)]
struct ErrorRow {
    #[serde(rename = "contexts.key")]
    contexts_key: Vec<String>,
    #[serde(rename = "contexts.value")]
    contexts_value: Vec<String>,
    culprit: String,
    deleted: u8,
    dist: Option<String>,
    environment: Option<String>,
    event_id: Uuid,
    #[serde(rename = "exception_frames.abs_path")]
    exception_frames_abs_path: Vec<Option<String>>,
    #[serde(rename = "exception_frames.colno")]
    exception_frames_colno: Vec<Option<u32>>,
    #[serde(rename = "exception_frames.filename")]
    exception_frames_filename: Vec<Option<String>>,
    #[serde(rename = "exception_frames.function")]
    exception_frames_function: Vec<Option<String>>,
    #[serde(rename = "exception_frames.in_app")]
    exception_frames_in_app: Vec<Option<u8>>,
    #[serde(rename = "exception_frames.lineno")]
    exception_frames_lineno: Vec<Option<u32>>,
    #[serde(rename = "exception_frames.module")]
    exception_frames_module: Vec<Option<String>>,
    #[serde(rename = "exception_frames.package")]
    exception_frames_package: Vec<Option<String>>,
    #[serde(rename = "exception_frames.stack_level")]
    exception_frames_stack_level: Vec<Option<u16>>,
    exception_main_thread: Option<u8>,
    #[serde(rename = "exception_stacks.mechanism_handled")]
    exception_stacks_mechanism_handled: Vec<Option<u8>>,
    #[serde(rename = "exception_stacks.mechanism_type")]
    exception_stacks_mechanism_type: Vec<Option<String>>,
    #[serde(rename = "exception_stacks.type")]
    exception_stacks_type: Vec<Option<String>>,
    #[serde(rename = "exception_stacks.value")]
    exception_stacks_value: Vec<Option<String>>,
    group_id: u64,
    hierarchical_hashes: Vec<Uuid>,
    http_method: Option<String>,
    http_referer: Option<String>,
    ip_address_v4: Option<Ipv4Addr>,
    ip_address_v6: Option<Ipv6Addr>,
    level: Option<String>,
    location: Option<String>,
    message_timestamp: f64,
    message: String,
    #[serde(rename = "modules.name")]
    modules_name: Vec<String>,
    #[serde(rename = "modules.version")]
    modules_version: Vec<String>,
    num_processing_errors: Option<u64>,
    offset: u64,
    partition: u16,
    platform: String,
    primary_hash: Uuid,
    project_id: u64,
    received: f64,
    release: Option<String>,
    replay_id: Option<Uuid>,
    retention_days: u16,
    sdk_integrations: Vec<String>,
    sdk_name: Option<String>,
    sdk_version: Option<String>,
    span_id: Option<u64>,
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
    timestamp: u32,
    title: String,
    trace_id: Option<Uuid>,
    trace_sampled: Option<u8>,
    transaction_hash: u64,
    transaction_name: String,
    #[serde(rename = "type")]
    ty: String,
    user_email: Option<String>,
    user_hash: u64,
    user_id: Option<String>,
    user_name: Option<String>,
    user: String,
    version: Option<String>,
}

impl TryFrom<ErrorMessage> for ErrorRow {
    type Error = anyhow::Error;

    fn try_from(from: ErrorMessage) -> anyhow::Result<ErrorRow> {
        // Unwrap the ip-address string.
        let ip_address_string = from.data.user.ip_address.unwrap_or_default();
        let (ip_address_v4, ip_address_v6) = match ip_address_string.parse::<IpAddr>() {
            Err(_) => (None, None),
            Ok(IpAddr::V4(ipv4)) => (Some(ipv4), None),
            Ok(IpAddr::V6(ipv6)) => (None, Some(ipv6)),
        };

        // Tags extraction.
        let mut transaction_name = None;
        let mut release = None;
        let mut dist = None;
        let mut user = None;
        let mut replay_id = None;
        let mut tags_key = Vec::with_capacity(from.data.tags.len());
        let mut tags_value = Vec::with_capacity(from.data.tags.len());

        for tag in from.data.tags.into_iter() {
            if &tag.0 == "transaction" {
                transaction_name = tag.1
            } else if &tag.0 == "sentry:release" {
                release = tag.1
            } else if &tag.0 == "sentry:dist" {
                dist = tag.1
            } else if &tag.0 == "sentry:user" {
                user = tag.1
            } else if &tag.0 == "replayId" {
                // TODO: empty state should be null?
                replay_id = tag
                    .1
                    .map(|v| Uuid::parse_str(&v).map(|v| v).unwrap_or_default())
            } else {
                tags_key.push(tag.0);
                tags_value.push(tag.1.unwrap_or_default());
            }
        }

        Ok(Self {
            transaction_name: transaction_name.unwrap_or_default(),
            release,
            dist,
            user: user.unwrap_or_default(),
            replay_id,
            tags_key,
            tags_value,
            ip_address_v4,
            ip_address_v6,
            group_id: from.group_id,
            sdk_name: from.data.sdk.name,
            sdk_version: from.data.sdk.version,
            sdk_integrations: from.data.sdk.integrations,
            timestamp: datetime_to_timestamp(from.timestamp).unwrap(),
            user_email: from.data.user.email,
            user_id: from.data.user.user_id,
            user_name: from.data.user.username,
            ..Default::default()
        })
    }
}

fn datetime_to_timestamp(datetime_str: Option<String>) -> Result<u32, &'static str> {
    let dt = match datetime_str {
        Some(v) => v,
        None => return Ok(Utc::now().timestamp() as u32),
    };

    // Datetimes must be provided in this format.
    let format = "%Y-%m-%dT%H:%M:%S%.fZ";

    // Parse the datetime string
    let datetime = DateTime::parse_from_str(&dt, format);

    // Check if parsing was successful
    match datetime {
        Ok(parsed_datetime) => {
            let timestamp = parsed_datetime.timestamp();
            if timestamp <= u32::MAX as i64 {
                Ok(timestamp as u32)
            } else {
                // If the timestamp is larger than a u32 can hold submit the current timestamp.
                Ok(Utc::now().timestamp() as u32)
            }
        }
        Err(_) => Err("Invalid datetime format"),
    }
}
