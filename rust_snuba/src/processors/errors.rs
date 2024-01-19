use anyhow::Context;
use anyhow::Error;
use chrono::DateTime;
use chrono::Utc;
use serde::de;
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
    row.message_timestamp = metadata.timestamp.timestamp() as u32;

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
    group_id: u64,
    project_id: u64,
    retention_days: u16,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorData {
    contexts: Contexts,
    culprit: String,
    errors: Option<Vec<Value>>,
    event_id: Uuid,
    #[serde(rename = "sentry.interfaces.Exception")]
    exception_alternate: Exception,
    exception: Exception,
    hierarchical_hashes: Vec<String>,
    location: Option<String>,
    message: String,
    modules: HashMap<String, Option<String>>,
    platform: String,
    primary_hash: String,
    received: f64,
    request: Request,
    sdk: Sdk,
    tags: Vec<(String, Option<String>)>,
    #[serde(rename = "sentry.interfaces.Threads")]
    thread_alternate: Thread,
    thread: Thread,
    title: String,
    ty: String,
    user: User, // Deviation: sentry.interfaces.User is not supported.
    version: Option<String>,
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
    trace_id: Option<Uuid>,
}

#[derive(Debug, Default, Deserialize)]
struct ReplayContext {
    #[serde(default)]
    replay_id: Option<Uuid>,
}

// Stacktraces

#[derive(Debug, Deserialize)]
struct Exception {
    #[serde(default)]
    values: Vec<ExceptionValue>, // Deviation: Apparently sent as Vec<Option<T>>.
}

#[derive(Debug, Deserialize)]
struct ExceptionValue {
    stacktrace: StrackTrace,
    #[serde(default)]
    mechanism: ExceptionMechanism,
    #[serde(default, rename = "type")]
    ty: Option<String>,
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    thread_id: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct ExceptionMechanism {
    #[serde(default, rename = "type")]
    ty: Option<String>,
    #[serde(default)]
    handled: Boolify,
}

#[derive(Debug, Deserialize)]
struct StrackTrace {
    #[serde(default)]
    frames: Vec<StrackFrame>,
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

// Threads

#[derive(Debug, Deserialize)]
struct Thread {
    #[serde(default)]
    values: Vec<ThreadValue>,
}

#[derive(Debug, Deserialize)]
struct ThreadValue {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    main: Option<bool>,
}

// SDK

#[derive(Debug, Deserialize)]
struct Sdk {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    integrations: Vec<Option<String>>,
}

// Request

#[derive(Debug, Deserialize)]
struct Request {
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
    exception_frames_stack_level: Vec<u16>, // Schema Deviation: Why would this ever be null?
    exception_main_thread: Option<u8>,
    #[serde(rename = "exception_stacks.mechanism_handled")]
    exception_stacks_mechanism_handled: Vec<Boolify>,
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
    message_timestamp: u32,
    message: String,
    #[serde(rename = "modules.name")]
    modules_name: Vec<String>,
    #[serde(rename = "modules.version")]
    modules_version: Vec<String>,
    num_processing_errors: u64,
    offset: u64,
    partition: u16,
    platform: String,
    primary_hash: Uuid,
    project_id: u64,
    received: u32,
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
        if from.data.ty == "transaction" {
            return Err(anyhow::Error::msg("Invalid type."));
        }

        // Hashes
        let primary_hash = to_uuid(from.data.primary_hash);
        let hierarchical_hashes: Vec<Uuid> = from
            .data
            .hierarchical_hashes
            .into_iter()
            .map(|v| to_uuid(v))
            .collect();

        // SDK Integrations
        let sdk_integrations = from
            .data
            .sdk
            .integrations
            .into_iter()
            .filter_map(|v| v)
            .collect();

        // Unwrap the ip-address string.
        let ip_address_string = from.data.user.ip_address.unwrap_or_default();
        let (ip_address_v4, ip_address_v6) = match ip_address_string.parse::<IpAddr>() {
            Err(_) => (None, None),
            Ok(IpAddr::V4(ipv4)) => (Some(ipv4), None),
            Ok(IpAddr::V6(ipv6)) => (None, Some(ipv6)),
        };

        // Extract HTTP referrer from the headers list.
        let mut http_referer = None;
        for (key, value) in from.data.request.headers {
            if key == "Referrer" {
                http_referer = value;
                break;
            }
        }

        // Modules.
        let mut module_names = Vec::with_capacity(from.data.modules.len());
        let mut module_versions = Vec::with_capacity(from.data.modules.len());
        for (name, version) in from.data.modules {
            module_names.push(name);
            module_versions.push(version.unwrap_or_default());
        }

        // Extract promoted tags.
        let mut environment = None;
        let mut level = None;
        let mut transaction_name = None;
        let mut release = None;
        let mut dist = None;
        let mut user = None;
        let mut replay_id = None;
        let mut tags_key = Vec::with_capacity(from.data.tags.len());
        let mut tags_value = Vec::with_capacity(from.data.tags.len());

        for tag in from.data.tags.into_iter() {
            if &tag.0 == "environment" {
                environment = tag.1
            } else if &tag.0 == "level" {
                level = tag.1
            } else if &tag.0 == "transaction" {
                transaction_name = tag.1
            } else if &tag.0 == "sentry:release" {
                release = tag.1
            } else if &tag.0 == "sentry:dist" {
                dist = tag.1
            } else if &tag.0 == "sentry:user" {
                user = tag.1
            } else if &tag.0 == "replayId" {
                // TODO: empty state should be null?
                replay_id = tag.1.map(|v| Uuid::parse_str(&v).unwrap_or_default())
            } else if let Some(tag_value) = tag.1 {
                // Only tags with non-null values are stored.
                tags_key.push(tag.0);
                tags_value.push(tag_value);
            }
        }

        // Conditionally overwrite replay_id if it was provided on the contexts object.
        match from.data.contexts.replay.replay_id {
            Some(rid) => replay_id = Some(rid),
            None => {}
        };

        // Stacktrace.

        // TODO: Add blacklist.
        // if output["project_id"] not in settings.PROJECT_STACKTRACE_BLACKLIST:

        let exception_count = from.data.exception.values.len();
        let frame_count = from
            .data
            .exception
            .values
            .iter()
            .map(|v| v.stacktrace.frames.len())
            .sum();

        let mut stack_level: u16 = 0;
        let mut stack_types = Vec::with_capacity(exception_count);
        let mut stack_values = Vec::with_capacity(exception_count);
        let mut stack_mechanism_types = Vec::with_capacity(exception_count);
        let mut stack_mechanism_handled = Vec::with_capacity(exception_count);
        let mut frame_abs_paths = Vec::with_capacity(frame_count);
        let mut frame_filenames = Vec::with_capacity(frame_count);
        let mut frame_packages = Vec::with_capacity(frame_count);
        let mut frame_modules = Vec::with_capacity(frame_count);
        let mut frame_functions = Vec::with_capacity(frame_count);
        let mut frame_in_app = Vec::with_capacity(frame_count);
        let mut frame_colnos = Vec::with_capacity(frame_count);
        let mut frame_linenos = Vec::with_capacity(frame_count);
        let mut frame_stack_levels = Vec::with_capacity(frame_count);
        let mut exception_main_thread = false;

        for stack in from.data.exception.values {
            stack_types.push(stack.ty);
            stack_values.push(stack.value);
            stack_mechanism_types.push(stack.mechanism.ty);
            stack_mechanism_handled.push(stack.mechanism.handled);

            for frame in stack.stacktrace.frames {
                frame_abs_paths.push(frame.abs_path);
                frame_filenames.push(frame.filename);
                frame_packages.push(frame.package);
                frame_modules.push(frame.module);
                frame_functions.push(frame.function);
                frame_in_app.push(frame.in_app.map(|v| v as u8));
                frame_colnos.push(frame.colno);
                frame_linenos.push(frame.lineno);
                frame_stack_levels.push(stack_level);
            }

            stack_level += 1;

            // We need to determine if the exception occurred on the main thread.
            if !exception_main_thread {
                if let Some(tid) = stack.thread_id {
                    for thread in &from.data.thread.values {
                        if let (Some(thread_id), Some(main)) = (thread.id, thread.main) {
                            if thread_id == tid && main {
                                exception_main_thread = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            culprit: from.data.culprit,
            dist,
            environment,
            event_id: from.data.event_id,
            exception_frames_abs_path: frame_abs_paths,
            exception_frames_colno: frame_colnos,
            exception_frames_filename: frame_filenames,
            exception_frames_function: frame_functions,
            exception_frames_in_app: frame_in_app,
            exception_frames_lineno: frame_linenos,
            exception_frames_module: frame_modules,
            exception_frames_package: frame_packages,
            exception_frames_stack_level: frame_stack_levels,
            exception_stacks_mechanism_handled: stack_mechanism_handled,
            exception_stacks_mechanism_type: stack_mechanism_types,
            exception_stacks_type: stack_types,
            exception_stacks_value: stack_values,
            group_id: from.group_id,
            hierarchical_hashes,
            http_method: from.data.request.method,
            http_referer,
            ip_address_v4,
            ip_address_v6,
            level,
            location: from.data.location,
            message: from.data.message,
            modules_name: module_names,
            modules_version: module_versions,
            num_processing_errors: from.data.errors.unwrap_or_default().len() as u64,
            platform: from.data.platform,
            primary_hash,
            project_id: from.project_id,
            received: from.data.received as u32, // TODO: Implicit rounding.
            release,
            replay_id,
            retention_days: from.retention_days,
            sdk_integrations,
            sdk_name: from.data.sdk.name,
            sdk_version: from.data.sdk.version,
            span_id: from.data.contexts.trace.span_id,
            tags_key,
            tags_value,
            timestamp: datetime_to_timestamp(from.timestamp).unwrap(),
            title: from.data.title,
            trace_id: from.data.contexts.trace.trace_id,
            trace_sampled: from.data.contexts.trace.sampled.map(|v| v as u8),
            transaction_name: transaction_name.unwrap_or_default(),
            ty: from.data.ty,
            user_email: from.data.user.email,
            user_id: from.data.user.user_id,
            user_name: from.data.user.username,
            user: user.unwrap_or_default(),
            version: from.data.version,
            ..Default::default()
        })
    }
}

fn to_uuid(uuid_string: String) -> Uuid {
    match Uuid::parse_str(&uuid_string) {
        Ok(uuid) => uuid,
        Err(_) => Uuid::from_slice(md5::compute(uuid_string.as_bytes()).as_slice()).unwrap(),
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
                Ok(Utc::now().timestamp() as u32)
            }
        }
        Err(_) => Err("Invalid datetime format"),
    }
}

#[derive(Debug, Default, Serialize)]
struct Boolify(Option<u8>);

impl<'de> Deserialize<'de> for Boolify {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Value::deserialize(deserializer)? {
            Value::Null => Ok(Boolify(None)),
            Value::Bool(v) => Ok(Boolify(Some(v as u8))),
            Value::String(v) => {
                if v == "yes" && v == "true" && v == "1" {
                    Ok(Boolify(Some(1)))
                } else if v == "no" && v == "false" && v == "0" {
                    Ok(Boolify(Some(0)))
                } else {
                    Ok(Boolify(None))
                }
            }
            _ => Ok(Boolify(None)),
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct Unicodify(Option<String>);

impl<'de> Deserialize<'de> for Unicodify {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Value::deserialize(deserializer)? {
            Value::Array(vs) => Ok(Unicodify(Some(
                serde_json::to_string(&vs).map_err(de::Error::custom)?,
            ))),
            Value::Bool(v) => Ok(Unicodify(Some(
                serde_json::to_string(&v).map_err(de::Error::custom)?,
            ))),
            Value::Null => Ok(Unicodify(None)),
            Value::Number(v) => Ok(Unicodify(Some(
                serde_json::to_string(&v).map_err(de::Error::custom)?,
            ))),
            Value::Object(v) => Ok(Unicodify(Some(
                serde_json::to_string(&v).map_err(de::Error::custom)?,
            ))),
            Value::String(v) => Ok(Unicodify(Some(v))),
        }
    }
}
