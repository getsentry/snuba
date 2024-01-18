use std::collections::HashMap;

use anyhow::Context;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    let msg = serde_json::from_slice(payload_bytes)?;
    let origin_timestamp = DateTime::from_timestamp(msg.received as i64, 0);
    let row = None;

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
    group_id: Uuid,
    platform: String,
    project_id: u64,
    retention_days: u16,
    timestamp: u32,
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
    tags: Vec<(String, Option<String>)>,
    title: String,
    ty: String,
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
    id: Option<String>,
    #[serde(default)]
    ip_address: Option<String>,
    #[serde(default)]
    username: Option<String>,
}
