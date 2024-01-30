use anyhow::Context;
use chrono::DateTime;
use serde::de;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

use rust_arroyo::backends::kafka::types::KafkaPayload;

use crate::config::ProcessorConfig;
use crate::processors::utils::{enforce_retention, ensure_valid_datetime};
use crate::types::{
    InsertBatch, InsertOrReplacement, KafkaMessageMetadata, ReplacementData, RowData,
};
use crate::EnvConfig;

pub fn process_message_with_replacement(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertOrReplacement<InsertBatch>> {
    // DEBUG DESERIALIZER. Uncomment this if you're getting Rust errors.
    //
    //let payload_bytes = payload.payload().context("Expected payload")?;
    //let msg: Vec<Value> = serde_json::from_slice(payload_bytes)?;
    //if msg.len() == 4 {
    //let x = msg.get(2).unwrap();
    //let y = x.to_string();
    //let msg: ErrorMessage = serde_json::from_str(&y)?;
    //}

    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: Message = serde_json::from_slice(payload_bytes)?;

    let (version, msg_type, error_event, replacement_event) = match msg {
        Message::FourTrain(version, msg_type, event, _state) => {
            (version, msg_type, Some(event), None)
        }
        Message::ThreeTrain(version, msg_type, event) => (version, msg_type, None, Some(event)),
    };

    if version != 2 {
        anyhow::bail!("Unsupported message version: {}", version);
    }

    match (msg_type.as_str(), error_event, replacement_event) {
        ("insert", Some(error), _) => {
            let origin_timestamp = DateTime::from_timestamp(error.data.received as i64, 0);

            let mut row = ErrorRow::parse(error, &config.env_config)?;
            row.partition = metadata.partition;
            row.offset = metadata.offset;
            row.message_timestamp = metadata.timestamp.timestamp() as u64;
            row.retention_days = Some(enforce_retention(row.retention_days, &config.env_config));

            Ok(InsertOrReplacement::Insert(InsertBatch {
                origin_timestamp,
                rows: RowData::from_rows([row])?,
                sentry_received_timestamp: None,
                cogs_data: None,
            }))
        }
        ("insert", None, _) => {
            anyhow::bail!("insert-event without an error payload");
        }
        (_, _, Some(replacement_event)) => Ok(InsertOrReplacement::Replacement(ReplacementData {
            key: replacement_event.project_id.to_string().into_bytes(),
            value: payload_bytes.clone(),
        })),
        _ => {
            anyhow::bail!("unsupported message format: {:?}", msg_type);
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Message {
    FourTrain(u8, String, ErrorMessage, Value),
    ThreeTrain(u8, String, ReplacementEvent),
}

#[derive(Deserialize, Debug)]
struct ReplacementEvent {
    project_id: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorMessage {
    data: ErrorData,
    #[serde(default, deserialize_with = "ensure_valid_datetime")]
    datetime: u32,
    event_id: Uuid,
    group_id: u64,
    message: String,
    primary_hash: String,
    project_id: u64,
    #[serde(default)]
    retention_days: Option<u16>,
    platform: String,
}

#[derive(Debug, Deserialize)]
struct ErrorData {
    #[serde(default)]
    contexts: Contexts,
    #[serde(default)]
    culprit: Unicodify,
    #[serde(default)]
    errors: Option<Vec<Value>>,
    #[serde(default, alias = "sentry.interfaces.Exception")]
    exception: Exception,
    #[serde(default)]
    hierarchical_hashes: Vec<String>,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    modules: HashMap<String, Option<String>>,
    #[serde(default)]
    received: f64,
    #[serde(default)]
    request: Request,
    #[serde(default)]
    sdk: Sdk,
    #[serde(default)]
    tags: Vec<Option<(Unicodify, Unicodify)>>,
    #[serde(default, alias = "sentry.interfaces.Threads")]
    threads: Option<Thread>,
    #[serde(default)]
    title: Unicodify,
    #[serde(default, rename = "type")]
    ty: Unicodify,
    #[serde(default, alias = "sentry.interfaces.User")]
    user: User,
    #[serde(default)]
    version: Option<String>,
}

// Contexts

type GenericContext = BTreeMap<String, ContextStringify>;

#[derive(Debug, Default, Deserialize)]
struct Contexts {
    #[serde(default)]
    replay: ReplayContext,
    #[serde(default)]
    trace: TraceContext,
    #[serde(flatten)]
    other: BTreeMap<String, GenericContext>,
}

#[derive(Debug, Default, Deserialize)]
struct TraceContext {
    #[serde(default)]
    sampled: Option<bool>,
    #[serde(default)]
    span_id: Option<String>,
    #[serde(default)]
    trace_id: Option<Uuid>,
    #[serde(flatten)]
    other: GenericContext,
}

#[derive(Debug, Default, Deserialize)]
struct ReplayContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replay_id: Option<Uuid>,
}

// Stacktraces

#[derive(Debug, Default, Deserialize)]
struct Exception {
    #[serde(default)]
    values: Option<Vec<Option<ExceptionValue>>>,
}

#[derive(Debug, Deserialize)]
struct ExceptionValue {
    stacktrace: StackTrace,
    #[serde(default)]
    mechanism: ExceptionMechanism,
    #[serde(default, rename = "type")]
    ty: Unicodify,
    #[serde(default)]
    value: Unicodify,
    #[serde(default)]
    thread_id: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct ExceptionMechanism {
    #[serde(default, rename = "type")]
    ty: Unicodify,
    #[serde(default)]
    handled: Boolify,
}

#[derive(Debug, Deserialize)]
struct StackTrace {
    #[serde(default)]
    frames: Vec<StackFrame>,
}

#[derive(Debug, Deserialize)]
struct StackFrame {
    #[serde(default)]
    abs_path: Unicodify,
    #[serde(default)]
    filename: Unicodify,
    #[serde(default)]
    package: Unicodify,
    #[serde(default)]
    module: Unicodify,
    #[serde(default)]
    function: Unicodify,
    #[serde(default)]
    in_app: Option<bool>,
    #[serde(default)]
    colno: Option<u32>,
    #[serde(default)]
    lineno: Option<u32>,
}

// Threads

#[derive(Debug, Default, Deserialize)]
struct Thread {
    #[serde(default)]
    values: Option<Vec<Option<ThreadValue>>>,
}

#[derive(Debug, Deserialize)]
struct ThreadValue {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    main: Option<bool>,
}

// SDK

#[derive(Debug, Default, Deserialize)]
struct Sdk {
    #[serde(default)]
    name: Unicodify,
    #[serde(default)]
    version: Unicodify,
    #[serde(default)]
    integrations: Option<Vec<Unicodify>>,
}

// Request

#[derive(Debug, Default, Deserialize)]
struct Request {
    #[serde(default)]
    method: Unicodify,
    #[serde(default)]
    headers: Vec<(String, Unicodify)>,
}

// User

#[derive(Debug, Default, Deserialize)]
struct User {
    #[serde(default)]
    email: Unicodify,
    #[serde(default)]
    id: Unicodify,
    #[serde(default)]
    ip_address: Option<String>,
    #[serde(default)]
    username: Unicodify,
    #[serde(default)]
    geo: GenericContext,
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
    exception_frames_in_app: Vec<Option<bool>>,
    #[serde(rename = "exception_frames.lineno")]
    exception_frames_lineno: Vec<Option<u32>>,
    #[serde(rename = "exception_frames.module")]
    exception_frames_module: Vec<Option<String>>,
    #[serde(rename = "exception_frames.package")]
    exception_frames_package: Vec<Option<String>>,
    #[serde(rename = "exception_frames.stack_level")]
    exception_frames_stack_level: Vec<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception_main_thread: Option<bool>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_address_v4: Option<Ipv4Addr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_address_v6: Option<Ipv6Addr>,
    level: Option<String>,
    location: Option<String>,
    message_timestamp: u64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    replay_id: Option<Uuid>,
    retention_days: Option<u16>,
    sdk_integrations: Vec<String>,
    sdk_name: String,
    sdk_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    span_id: Option<u64>,
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
    timestamp: u32,
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_sampled: Option<u8>,
    transaction_name: String,
    #[serde(rename = "type")]
    ty: String,
    user_email: Option<String>,
    user_id: Option<String>,
    user_name: Option<String>,
    user: String,
    version: Option<String>,
}

impl ErrorRow {
    fn parse(from: ErrorMessage, config: &EnvConfig) -> anyhow::Result<ErrorRow> {
        if from.data.ty.0 == Some("transaction".to_string()) {
            return Err(anyhow::Error::msg("Invalid type."));
        }

        // Parse the optional string to a base16 u64.
        let span_id = from
            .data
            .contexts
            .trace
            .span_id
            .as_ref()
            .map(|inner| u64::from_str_radix(inner, 16).ok())
            .unwrap_or_default();

        // Hashes
        let primary_hash = to_uuid(from.primary_hash);
        let hierarchical_hashes: Vec<Uuid> = from
            .data
            .hierarchical_hashes
            .into_iter()
            .map(to_uuid)
            .collect();

        // SDK Integrations
        let sdk_integrations = from
            .data
            .sdk
            .integrations
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.0)
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
                http_referer = value.0;
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

        let mut from_tags = from.data.tags;
        from_tags.sort();

        for t in from_tags.into_iter().flatten() {
            if let (Some(tag_key), Some(tag_value)) = (&t.0 .0, &t.1 .0) {
                if tag_key == "environment" {
                    environment = Some(tag_value.clone());
                } else if tag_key == "level" {
                    level = Some(tag_value.clone());
                } else if tag_key == "transaction" {
                    transaction_name = Some(tag_value.clone());
                } else if tag_key == "sentry:release" {
                    release = Some(tag_value.clone());
                } else if tag_key == "sentry:dist" {
                    dist = Some(tag_value.clone());
                } else if tag_key == "sentry:user" {
                    user = Some(tag_value.to_owned());
                } else if tag_key == "replayId" {
                    replay_id = Uuid::parse_str(tag_value).ok();
                }

                tags_key.push(tag_key.to_owned());
                tags_value.push(tag_value.clone());
            }
        }

        // Arbitrary capacity. Could be computed exactly from the types but the types
        // could change. This does not use so much memory and gives us significant performance
        // improvement.
        let mut contexts_keys = Vec::with_capacity(100);
        let mut contexts_values = Vec::with_capacity(100);

        let mut other_contexts = from.data.contexts.other;
        if !from.data.user.geo.is_empty() {
            other_contexts.insert("geo".to_owned(), from.data.user.geo);
        }

        for (container_name, container) in other_contexts {
            for (key, value) in container {
                if let Some(v) = value.0 {
                    if key != "type" {
                        contexts_keys.push(format!("{}.{}", container_name, key));
                        contexts_values.push(v);
                    }
                }
            }
        }

        // XXX: we only extract trace context into extra contexts for exact compatibility with the
        // python processor. some fields may be used in queries, but other fields can probably go
        // since they have already been promoted.
        if let Some(ContextStringify(Some(value))) =
            from.data.contexts.trace.other.get("client_sample_rate")
        {
            contexts_keys.push("trace.client_sample_rate".to_owned());
            contexts_values.push(value.to_string());
        }

        if let Some(ContextStringify(Some(value))) = from.data.contexts.trace.other.get("op") {
            contexts_keys.push("trace.op".to_owned());
            contexts_values.push(value.to_string());
        }

        if let Some(span_id) = from.data.contexts.trace.span_id {
            contexts_keys.push("trace.span_id".to_owned());
            contexts_values.push(span_id.to_string());
        }

        if let Some(ContextStringify(Some(status))) = from.data.contexts.trace.other.get("status") {
            contexts_keys.push("trace.status".to_owned());
            contexts_values.push(status.to_string());
        }

        if let Some(trace_id) = from.data.contexts.trace.trace_id {
            contexts_keys.push("trace.trace_id".to_owned());
            contexts_values.push(trace_id.simple().to_string());
        }

        // Conditionally overwrite replay_id if it was provided on the contexts object.
        if let Some(rid) = from.data.contexts.replay.replay_id {
            replay_id = Some(rid)
        }

        // Stacktrace.

        let exceptions = from.data.exception.values.unwrap_or_default();

        let exception_count = exceptions.len();
        let frame_count = exceptions
            .iter()
            .filter_map(|v| Some(v.as_ref()?.stacktrace.frames.len()))
            .sum();

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
        let mut exception_main_thread: Option<bool> = None;

        let threads = from.data.threads.unwrap_or_default();

        if !config
            .project_stacktrace_blacklist
            .contains(&from.project_id)
        {
            for (stack_level, stack) in exceptions.into_iter().flatten().enumerate() {
                stack_types.push(stack.ty.0);
                stack_values.push(stack.value.0);
                stack_mechanism_types.push(stack.mechanism.ty.0);
                stack_mechanism_handled.push(stack.mechanism.handled.0);

                for frame in stack.stacktrace.frames {
                    frame_abs_paths.push(frame.abs_path.0);
                    frame_filenames.push(frame.filename.0);
                    frame_packages.push(frame.package.0);
                    frame_modules.push(frame.module.0);
                    frame_functions.push(frame.function.0);
                    frame_in_app.push(frame.in_app);
                    frame_colnos.push(frame.colno);
                    frame_linenos.push(frame.lineno);
                    frame_stack_levels.push(stack_level as u16);
                }

                // We need to determine if the exception occurred on the main thread.
                if exception_main_thread != Some(true) {
                    if let Some(stack_thread) = stack.thread_id {
                        for thread in threads.values.iter().flatten().filter_map(|x| x.as_ref()) {
                            if let (Some(thread_id), Some(main)) = (thread.id, thread.main) {
                                if thread_id == stack_thread && main {
                                    // if it's the main thread, mark it as such and stop it
                                    exception_main_thread = Some(true);
                                    break;
                                } else {
                                    // if it's NOT the main thread, mark it as such, but
                                    // keep looking for the main thread
                                    exception_main_thread = Some(false);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            contexts_key: contexts_keys,
            contexts_value: contexts_values,
            culprit: from.data.culprit.0.unwrap_or_default(),
            deleted: 0,
            dist,
            environment,
            event_id: from.event_id,
            exception_frames_abs_path: frame_abs_paths,
            exception_frames_colno: frame_colnos,
            exception_frames_filename: frame_filenames,
            exception_frames_function: frame_functions,
            exception_frames_in_app: frame_in_app,
            exception_frames_lineno: frame_linenos,
            exception_frames_module: frame_modules,
            exception_frames_package: frame_packages,
            exception_frames_stack_level: frame_stack_levels,
            exception_main_thread,
            exception_stacks_mechanism_handled: stack_mechanism_handled,
            exception_stacks_mechanism_type: stack_mechanism_types,
            exception_stacks_type: stack_types,
            exception_stacks_value: stack_values,
            group_id: from.group_id,
            hierarchical_hashes,
            http_method: from.data.request.method.0,
            http_referer,
            ip_address_v4,
            ip_address_v6,
            level,
            location: from.data.location,
            message: from.message,
            modules_name: module_names,
            modules_version: module_versions,
            num_processing_errors: from.data.errors.unwrap_or_default().len() as u64,
            platform: from.platform,
            primary_hash,
            project_id: from.project_id,
            received: from.data.received as u32, // TODO: Implicit truncation.
            release,
            replay_id,
            retention_days: from.retention_days,
            sdk_integrations,
            sdk_name: from.data.sdk.name.0.unwrap_or_default(),
            sdk_version: from.data.sdk.version.0.unwrap_or_default(),
            span_id,
            tags_key,
            tags_value,
            timestamp: from.datetime,
            title: from.data.title.0.unwrap_or_default(),
            trace_id: from.data.contexts.trace.trace_id,
            trace_sampled: from.data.contexts.trace.sampled.map(|v| v as u8),
            transaction_name: transaction_name.unwrap_or_default(),
            ty: from.data.ty.0.unwrap_or_default(),
            user_email: from.data.user.email.0,
            user_id: from.data.user.id.0,
            user_name: from.data.user.username.0,
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

#[derive(Debug, Default)]
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

#[derive(Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
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
            Value::Bool(v) => Ok(Unicodify(Some(v.to_string()))),
            Value::Null => Ok(Unicodify(None)),
            Value::Number(v) => Ok(Unicodify(Some(v.to_string()))),
            Value::Object(v) => Ok(Unicodify(Some(
                serde_json::to_string(&v).map_err(de::Error::custom)?,
            ))),
            Value::String(v) => Ok(Unicodify(Some(v))),
        }
    }
}

#[derive(Debug, Default)]
struct ContextStringify(Option<String>);

impl<'de> Deserialize<'de> for ContextStringify {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Value::deserialize(deserializer)? {
            Value::String(v) => Ok(ContextStringify(Some(v))),
            Value::Number(v) => Ok(ContextStringify(Some(v.to_string()))),
            Value::Bool(v) => Ok(ContextStringify(Some(
                if v { "True" } else { "False" }.to_owned(),
            ))),
            _ => Ok(ContextStringify(None)),
        }
    }
}
