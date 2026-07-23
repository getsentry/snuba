use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use anyhow::{anyhow, bail, Context};
use chrono::NaiveDateTime;
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use uuid::Uuid;

use sentry_arroyo::backends::kafka::types::KafkaPayload;

use crate::config::{EnvConfig, ProcessorConfig};
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

const FINGERPRINTS_HARD_LIMIT_SIZE: usize = 100;

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: Message = serde_json::from_slice(payload_bytes).with_context(|| {
        format!(
            "payload start: {}",
            String::from_utf8_lossy(&payload_bytes[..payload_bytes.len().min(200)])
        )
    })?;

    if msg.version != 2 {
        bail!("Unsupported message version: {}", msg.version);
    }
    if msg.operation != "insert" {
        bail!("Invalid message type: {}", msg.operation);
    }

    let row = SearchIssuesRow::parse(msg.event, &metadata, &config.env_config)?;
    InsertBatch::from_rows([row], None)
}

#[derive(Debug)]
struct Message {
    version: u8,
    operation: String,
    event: InsertEvent,
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = Message;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a sequence of [version, operation, event, ...]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Message, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let version = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let operation = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let event = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Message {
                    version,
                    operation,
                    event,
                })
            }
        }

        deserializer.deserialize_seq(MessageVisitor)
    }
}

#[derive(Debug, Deserialize)]
struct InsertEvent {
    organization_id: u64,
    project_id: u64,
    group_id: u64,
    #[serde(default)]
    group_first_seen: Option<String>,
    event_id: String,
    primary_hash: String,
    platform: String,
    message: String,
    #[serde(default)]
    datetime: Option<String>,
    #[serde(default)]
    retention_days: Option<u16>,
    data: EventData,
    occurrence_data: OccurrenceData,
}

#[derive(Debug, Deserialize)]
struct EventData {
    received: f64,
    #[serde(default)]
    client_timestamp: Option<f64>,
    #[serde(default)]
    timestamp: Option<Value>,
    #[serde(default)]
    start_timestamp: Option<Value>,
    #[serde(default)]
    tags: Option<MapOrPairs>,
    #[serde(default)]
    user: Option<UserData>,
    #[serde(default)]
    sdk: Option<Sdk>,
    #[serde(default)]
    contexts: Option<Contexts>,
    #[serde(default)]
    request: Option<Request>,
    #[serde(default)]
    environment: Option<Value>,
    #[serde(default)]
    release: Option<Value>,
    #[serde(default)]
    dist: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OccurrenceData {
    id: String,
    #[serde(rename = "type")]
    type_id: u16,
    issue_title: String,
    fingerprint: Vec<String>,
    detection_time: f64,
    #[serde(default)]
    subtitle: Option<String>,
    #[serde(default)]
    culprit: Option<String>,
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    resource_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct UserData {
    #[serde(default)]
    id: Option<Value>,
    #[serde(default)]
    username: Option<Value>,
    #[serde(default)]
    email: Option<Value>,
    #[serde(default)]
    ip_address: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
struct Sdk {
    #[serde(default)]
    name: Option<Value>,
    #[serde(default)]
    version: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
struct Request {
    #[serde(default)]
    method: Option<Value>,
    #[serde(default)]
    headers: Option<MapOrPairs>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MapOrPairs {
    Map(BTreeMap<String, Value>),
    Pairs(Vec<Option<Vec<Value>>>),
}

impl MapOrPairs {
    fn into_map(self) -> BTreeMap<String, Value> {
        match self {
            MapOrPairs::Map(map) => map,
            MapOrPairs::Pairs(pairs) => {
                let mut map = BTreeMap::new();
                for pair in pairs.into_iter().flatten() {
                    if let [key, value, ..] = pair.as_slice() {
                        if let Some(key) = stringify_value(key) {
                            map.insert(key, value.clone());
                        }
                    }
                }
                map
            }
        }
    }
}

#[derive(Debug, Default)]
struct Contexts(Vec<(String, ContextValue)>);

impl<'de> Deserialize<'de> for Contexts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ContextsVisitor;

        impl<'de> Visitor<'de> for ContextsVisitor {
            type Value = Contexts;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a contexts map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Contexts, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut out = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, ContextValue>()? {
                    out.push((key, value));
                }
                Ok(Contexts(out))
            }
        }

        deserializer.deserialize_map(ContextsVisitor)
    }
}

#[derive(Debug)]
enum ContextValue {
    Map(Vec<(String, Value)>),
    Other,
}

impl<'de> Deserialize<'de> for ContextValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ContextValueVisitor;

        impl<'de> Visitor<'de> for ContextValueVisitor {
            type Value = ContextValue;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any context value")
            }

            fn visit_map<A>(self, mut map: A) -> Result<ContextValue, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut out = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    out.push((key, value));
                }
                Ok(ContextValue::Map(out))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<ContextValue, A::Error>
            where
                A: SeqAccess<'de>,
            {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(ContextValue::Other)
            }

            fn visit_str<E>(self, _v: &str) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_string<E>(self, _v: String) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_bool<E>(self, _v: bool) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_i64<E>(self, _v: i64) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_u64<E>(self, _v: u64) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_f64<E>(self, _v: f64) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_none<E>(self) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
            fn visit_unit<E>(self) -> Result<ContextValue, E> {
                Ok(ContextValue::Other)
            }
        }

        deserializer.deserialize_any(ContextValueVisitor)
    }
}

#[derive(Debug, Default, Serialize)]
struct SearchIssuesRow {
    organization_id: u64,
    project_id: u64,
    group_id: u64,
    group_first_seen: Option<u32>,
    event_id: Uuid,
    search_title: String,
    primary_hash: Uuid,
    fingerprint: Vec<String>,
    occurrence_id: Uuid,
    occurrence_type_id: u16,
    detection_timestamp: u32,
    resource_id: Option<String>,
    message: String,
    subtitle: Option<String>,
    culprit: Option<String>,
    level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<Uuid>,
    platform: String,
    environment: Option<String>,
    release: Option<String>,
    dist: Option<String>,
    receive_timestamp: u32,
    client_timestamp: u32,
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
    user: Option<String>,
    user_id: Option<String>,
    user_name: Option<String>,
    user_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_address_v4: Option<Ipv4Addr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_address_v6: Option<Ipv6Addr>,
    sdk_name: Option<String>,
    sdk_version: Option<String>,
    #[serde(rename = "contexts.key")]
    contexts_key: Vec<String>,
    #[serde(rename = "contexts.value")]
    contexts_value: Vec<String>,
    http_method: Option<String>,
    http_referer: Option<String>,
    transaction_duration: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replay_id: Option<Uuid>,
    message_timestamp: u32,
    partition: u16,
    offset: u64,
    retention_days: u16,
    timestamp_ms: u64,
}

impl SearchIssuesRow {
    fn parse(
        event: InsertEvent,
        metadata: &KafkaMessageMetadata,
        env_config: &EnvConfig,
    ) -> anyhow::Result<Self> {
        let data = event.data;
        let occ = event.occurrence_data;

        let detection_timestamp = seconds_from_timestamp(occ.detection_time);
        let receive_timestamp = seconds_from_timestamp(data.received);
        let retention_days = enforce_retention(event.retention_days, env_config);

        let (client_timestamp, timestamp_ms) = match data.client_timestamp.filter(|c| *c != 0.0) {
            Some(client_ts) => (
                clamp_u32(client_ts as i64),
                (client_ts * 1000.0).round().max(0.0) as u64,
            ),
            None => {
                let datetime_str = event
                    .datetime
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        anyhow!("message missing data.client_timestamp or datetime field")
                    })?;
                let naive = NaiveDateTime::parse_from_str(datetime_str, PAYLOAD_DATETIME_FORMAT)
                    .with_context(|| {
                        format!("datetime field has incompatible datetime format: {datetime_str}")
                    })?;
                let dt = naive.and_utc();
                let secs = dt.timestamp();
                if !(0..=u32::MAX as i64).contains(&secs) {
                    bail!("datetime field out of valid range: {datetime_str}");
                }
                (secs as u32, dt.timestamp_millis().max(0) as u64)
            }
        };

        let group_first_seen = match event.group_first_seen {
            Some(raw) => {
                let naive = NaiveDateTime::parse_from_str(&raw, PAYLOAD_DATETIME_FORMAT)
                    .with_context(|| format!("group_first_seen has incompatible format: {raw}"))?;
                let secs = naive.and_utc().timestamp();
                if (0..=u32::MAX as i64).contains(&secs) {
                    Some(secs as u32)
                } else {
                    None
                }
            }
            None => None,
        };

        let mut fingerprint = occ.fingerprint;
        fingerprint.truncate(FINGERPRINTS_HARD_LIMIT_SIZE - 1);

        let tags_map = data.tags.map(MapOrPairs::into_map).unwrap_or_default();
        let mut tags_key = Vec::with_capacity(tags_map.len());
        let mut tags_value = Vec::with_capacity(tags_map.len());
        for (key, value) in &tags_map {
            if let Some(unicodified) = stringify_value(value) {
                if !unicodified.is_empty() {
                    tags_key.push(key.clone());
                    tags_value.push(unicodified);
                }
            }
        }

        let environment = match tags_map.get("environment") {
            Some(value) => stringify_value(value),
            None => data.environment.as_ref().and_then(stringify_value),
        };
        let release = match tags_map.get("sentry:release") {
            Some(value) => stringify_value(value),
            None => data.release.as_ref().and_then(stringify_value),
        };
        let user = tags_map.get("sentry:user").and_then(stringify_value);
        let dist = match tags_map.get("sentry:dist") {
            Some(value) => stringify_value(value),
            None => data.dist.as_ref().and_then(stringify_value),
        };

        let user_data = data.user.unwrap_or_default();
        let user_id = user_data.id.as_ref().and_then(stringify_value);
        let user_name = user_data.username.as_ref().and_then(stringify_value);
        let user_email = user_data.email.as_ref().and_then(stringify_value);
        let (ip_address_v4, ip_address_v6) = match user_data
            .ip_address
            .as_ref()
            .and_then(stringify_value)
            .and_then(|s| s.parse::<IpAddr>().ok())
        {
            Some(IpAddr::V4(v4)) => (Some(v4), None),
            Some(IpAddr::V6(v6)) => (None, Some(v6)),
            None => (None, None),
        };

        let sdk = data.sdk.unwrap_or_default();
        let sdk_name = sdk.name.as_ref().and_then(stringify_value);
        let sdk_version = sdk.version.as_ref().and_then(stringify_value);

        let request = data.request.unwrap_or_default();
        let http_method = request.method.as_ref().and_then(stringify_value);
        let headers_map = request
            .headers
            .map(MapOrPairs::into_map)
            .unwrap_or_default();
        let http_referer = headers_map.get("Referer").and_then(stringify_value);

        let contexts = data.contexts.unwrap_or_default();
        let mut contexts_key = Vec::new();
        let mut contexts_value = Vec::new();
        for (name, value) in &contexts.0 {
            if let ContextValue::Map(inner) = value {
                for (inner_key, inner_value) in inner {
                    if inner_key == "type" {
                        continue;
                    }
                    if let Some(stringified) = stringify_scalar(inner_value) {
                        contexts_key.push(format!("{name}.{inner_key}"));
                        contexts_value.push(stringified);
                    }
                }
            }
        }

        let trace_id = promote_uuid_context(&contexts, "trace", "trace_id")?;
        let profile_id = promote_uuid_context(&contexts, "profile", "profile_id")?;
        let replay_id = promote_uuid_context(&contexts, "replay", "replay_id")?;

        let transaction_duration = match (
            value_as_number(&data.start_timestamp),
            value_as_number(&data.timestamp),
        ) {
            (Some(start), Some(finish)) => {
                ((finish - start) * 1000.0).clamp(0.0, u32::MAX as f64) as u32
            }
            _ => 0,
        };

        Ok(SearchIssuesRow {
            organization_id: event.organization_id,
            project_id: event.project_id,
            group_id: event.group_id,
            group_first_seen,
            event_id: parse_uuid(&event.event_id)?,
            search_title: occ.issue_title,
            primary_hash: parse_uuid(&event.primary_hash)?,
            fingerprint,
            occurrence_id: parse_uuid(&occ.id)?,
            occurrence_type_id: occ.type_id,
            detection_timestamp,
            resource_id: occ.resource_id,
            message: event.message,
            subtitle: occ.subtitle,
            culprit: occ.culprit,
            level: occ.level,
            trace_id,
            platform: event.platform,
            environment,
            release,
            dist,
            receive_timestamp,
            client_timestamp,
            tags_key,
            tags_value,
            user,
            user_id,
            user_name,
            user_email,
            ip_address_v4,
            ip_address_v6,
            sdk_name,
            sdk_version,
            contexts_key,
            contexts_value,
            http_method,
            http_referer,
            transaction_duration,
            profile_id,
            replay_id,
            message_timestamp: clamp_u32(metadata.timestamp.timestamp()),
            partition: metadata.partition,
            offset: metadata.offset,
            retention_days,
            timestamp_ms,
        })
    }
}

fn promote_uuid_context(
    contexts: &Contexts,
    context_name: &str,
    key: &str,
) -> anyhow::Result<Option<Uuid>> {
    for (name, value) in &contexts.0 {
        if name != context_name {
            continue;
        }
        if let ContextValue::Map(inner) = value {
            for (inner_key, inner_value) in inner {
                if inner_key == key && !inner_value.is_null() {
                    if let Some(stringified) = stringify_value(inner_value) {
                        return Ok(Some(parse_uuid(&stringified)?));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn parse_uuid(value: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| anyhow!("invalid UUID: {value}"))
}

fn stringify_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn stringify_scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::String(_) => None,
        Value::Number(_) | Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_number(value: &Option<Value>) -> Option<f64> {
    match value {
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

fn seconds_from_timestamp(value: f64) -> u32 {
    clamp_u32(value.trunc() as i64)
}

fn clamp_u32(value: i64) -> u32 {
    value.clamp(0, u32::MAX as i64) as u32
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use serde_json::json;

    fn kafka_meta() -> KafkaMessageMetadata {
        KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from_timestamp(1_234_567_890, 0).unwrap(),
        }
    }

    fn base_event() -> Value {
        json!({
            "project_id": 1,
            "organization_id": 2,
            "group_id": 3,
            "event_id": "7f0e2b1a4c5d4e6f8a9b0c1d2e3f4a5b",
            "retention_days": 90,
            "primary_hash": "a1b2c3d4e5f6071829304a5b6c7d8e9f",
            "datetime": "2023-06-27T00:00:00.000000Z",
            "platform": "other",
            "message": "something",
            "data": {
                "received": 1687800001.0
            },
            "occurrence_data": {
                "id": "cccccccccccccccccccccccccccccccc",
                "type": 1,
                "issue_title": "search me",
                "fingerprint": ["one", "two"],
                "detection_time": 1687800000.0
            }
        })
    }

    fn process(event: Value) -> Vec<Value> {
        let msg = json!([2, "insert", event]);
        let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
        let batch = process_message(payload, kafka_meta(), &ProcessorConfig::default()).unwrap();
        let encoded = String::from_utf8(batch.rows.into_encoded_rows()).unwrap();
        encoded
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn process_one(event: Value) -> Value {
        let mut rows = process(event);
        assert_eq!(rows.len(), 1);
        rows.remove(0)
    }

    fn process_one_raw(event_json: &str) -> Value {
        let msg = format!("[2, \"insert\", {event_json}]");
        let payload = KafkaPayload::new(None, None, Some(msg.into_bytes()));
        let batch = process_message(payload, kafka_meta(), &ProcessorConfig::default()).unwrap();
        let encoded = String::from_utf8(batch.rows.into_encoded_rows()).unwrap();
        let mut rows: Vec<Value> = encoded
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        rows.remove(0)
    }

    #[test]
    fn test_basic_required_columns() {
        let row = process_one(base_event());
        assert_eq!(row["organization_id"], 2);
        assert_eq!(row["project_id"], 1);
        assert_eq!(row["group_id"], 3);
        assert_eq!(row["search_title"], "search me");
        assert_eq!(row["occurrence_type_id"], 1);
        assert_eq!(row["message"], "something");
        assert_eq!(row["platform"], "other");
        assert_eq!(row["fingerprint"], json!(["one", "two"]));
        assert_eq!(row["retention_days"], 90);
        assert_eq!(row["partition"], 0);
        assert_eq!(row["offset"], 1);
        assert_eq!(row["event_id"], "7f0e2b1a-4c5d-4e6f-8a9b-0c1d2e3f4a5b");
        assert_eq!(row["occurrence_id"], "cccccccc-cccc-cccc-cccc-cccccccccccc");
        assert_eq!(row["detection_timestamp"], 1687800000u32);
        assert_eq!(row["receive_timestamp"], 1687800001u32);
        assert_eq!(row["message_timestamp"], 1_234_567_890u32);
    }

    #[test]
    fn test_client_timestamp_and_timestamp_ms_from_datetime() {
        let mut event = base_event();
        event["datetime"] = json!("2023-02-27T15:40:12.223000Z");
        let row = process_one(event);
        assert_eq!(row["client_timestamp"], 1_677_512_412u32);
        assert_eq!(row["timestamp_ms"], 1_677_512_412_223u64);
    }

    #[test]
    fn test_client_timestamp_from_data() {
        let mut event = base_event();
        event["data"]["client_timestamp"] = json!(1_677_512_412.223);
        let row = process_one(event);
        assert_eq!(row["client_timestamp"], 1_677_512_412u32);
        assert_eq!(row["timestamp_ms"], 1_677_512_412_223u64);
    }

    #[test]
    fn test_missing_client_timestamp_and_datetime_errors() {
        let mut event = base_event();
        event.as_object_mut().unwrap().remove("datetime");
        let msg = json!([2, "insert", event]);
        let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
        let result = process_message(payload, kafka_meta(), &ProcessorConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_user() {
        let mut event = base_event();
        event["data"]["user"] = json!({
            "id": 1,
            "username": "user",
            "email": "test@example.com",
            "ip_address": "127.0.0.1"
        });
        let row = process_one(event);
        assert_eq!(row["user_name"], "user");
        assert_eq!(row["user_id"], "1");
        assert_eq!(row["user_email"], "test@example.com");
        assert_eq!(row["ip_address_v4"], "127.0.0.1");
        assert!(row.get("ip_address_v6").is_none());
    }

    #[test]
    fn test_extract_user_empty() {
        let mut event = base_event();
        event["data"]["user"] = json!({});
        let row = process_one(event);
        assert_eq!(row["user_name"], Value::Null);
        assert_eq!(row["user_id"], Value::Null);
        assert_eq!(row["user_email"], Value::Null);
        assert!(row.get("ip_address_v4").is_none());
    }

    #[test]
    fn test_extract_ipv6() {
        let mut event = base_event();
        event["data"]["user"] = json!({ "ip_address": "2001:db8::1" });
        let row = process_one(event);
        assert_eq!(row["ip_address_v6"], "2001:db8::1");
        assert!(row.get("ip_address_v4").is_none());
    }

    #[test]
    fn test_promoted_user_from_tag() {
        let mut event = base_event();
        event["data"]["tags"] = json!({ "sentry:user": "user123" });
        let row = process_one(event);
        assert_eq!(row["user"], "user123");
    }

    #[test]
    fn test_extract_environment() {
        let mut event = base_event();
        event["data"]["environment"] = json!("prod");
        let row = process_one(event);
        assert_eq!(row["environment"], "prod");
    }

    #[test]
    fn test_extract_environment_from_tag() {
        let mut event = base_event();
        event["data"]["environment"] = json!("prod");
        event["data"]["tags"] = json!({ "environment": "dev" });
        let row = process_one(event);
        assert_eq!(row["environment"], "dev");
    }

    #[test]
    fn test_extract_release_from_tag() {
        let mut event = base_event();
        event["data"]["release"] = json!("release@123");
        event["data"]["tags"] = json!({ "sentry:release": "release@456" });
        let row = process_one(event);
        assert_eq!(row["release"], "release@456");
    }

    #[test]
    fn test_extract_dist_from_tag() {
        let mut event = base_event();
        event["data"]["dist"] = json!("dist@123");
        event["data"]["tags"] = json!({ "sentry:dist": "dist@456" });
        let row = process_one(event);
        assert_eq!(row["dist"], "dist@456");
    }

    #[test]
    fn test_extract_tags_sorted() {
        let mut event = base_event();
        event["data"]["tags"] = json!({
            "key": "value",
            "key4": "value4",
            "key3": "value3",
            "key2": "value2"
        });
        let row = process_one(event);
        assert_eq!(row["tags.key"], json!(["key", "key2", "key3", "key4"]));
        assert_eq!(
            row["tags.value"],
            json!(["value", "value2", "value3", "value4"])
        );
    }

    #[test]
    fn test_extract_tags_from_list() {
        let mut event = base_event();
        event["data"]["tags"] = json!([["level", "error"], ["environment", "production"]]);
        let row = process_one(event);
        assert_eq!(row["tags.key"], json!(["environment", "level"]));
        assert_eq!(row["tags.value"], json!(["production", "error"]));
        assert_eq!(row["environment"], "production");
    }

    #[test]
    fn test_extract_http() {
        let mut event = base_event();
        event["data"]["request"] = json!({
            "method": "GET",
            "headers": [["Referer", "http://example.com"], ["User-Agent", "test"]],
            "extra_stuff": "not_used"
        });
        let row = process_one(event);
        assert_eq!(row["http_method"], "GET");
        assert_eq!(row["http_referer"], "http://example.com");
    }

    #[test]
    fn test_extract_sdk() {
        let mut event = base_event();
        event["data"]["sdk"] = json!({
            "version": "1.2.3",
            "name": "python",
            "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}]
        });
        let row = process_one(event);
        assert_eq!(row["sdk_name"], "python");
        assert_eq!(row["sdk_version"], "1.2.3");
    }

    #[test]
    fn test_extract_context_null_dicts() {
        let mut event = base_event();
        event["data"]["contexts"] = json!({
            "trace": null,
            "profile": null,
            "replay": null,
            "scalar": {"string": "scalar_value"}
        });
        let row = process_one(event);
        assert_eq!(row["contexts.key"], json!(["scalar.string"]));
        assert_eq!(row["contexts.value"], json!(["scalar_value"]));
    }

    #[test]
    fn test_extract_context_filters_non_dict_preserves_order() {
        let row = process_one_raw(
            r#"{
                "project_id": 1,
                "organization_id": 2,
                "group_id": 3,
                "event_id": "7f0e2b1a4c5d4e6f8a9b0c1d2e3f4a5b",
                "retention_days": 90,
                "primary_hash": "a1b2c3d4e5f6071829304a5b6c7d8e9f",
                "datetime": "2023-06-27T00:00:00.000000Z",
                "platform": "other",
                "message": "something",
                "data": {
                    "received": 1687800001.0,
                    "contexts": {
                        "string": "blah",
                        "int": 1,
                        "float": 1.1,
                        "array": ["a", "b", "c"],
                        "scalar": {
                            "string": "scalar_value",
                            "int": 99,
                            "float": 123.111
                        },
                        "nested_dict": {
                            "array": [1, 2, 3],
                            "dict": {"key1": "value1"},
                            "string": "blah_nested",
                            "int": 2,
                            "float": 2.2
                        }
                    }
                },
                "occurrence_data": {
                    "id": "cccccccccccccccccccccccccccccccc",
                    "type": 1,
                    "issue_title": "search me",
                    "fingerprint": ["one", "two"],
                    "detection_time": 1687800000.0
                }
            }"#,
        );
        assert_eq!(
            row["contexts.key"],
            json!([
                "scalar.string",
                "scalar.int",
                "scalar.float",
                "nested_dict.string",
                "nested_dict.int",
                "nested_dict.float"
            ])
        );
        assert_eq!(
            row["contexts.value"],
            json!(["scalar_value", "99", "123.111", "blah_nested", "2", "2.2"])
        );
    }

    #[test]
    fn test_extract_trace_id_from_contexts() {
        let mut event = base_event();
        event["data"]["contexts"] =
            json!({ "trace": {"trace_id": "1234567890abcdef1234567890abcdef"} });
        let row = process_one(event);
        assert_eq!(row["trace_id"], "12345678-90ab-cdef-1234-567890abcdef");

        for invalid in [
            json!(""),
            json!("im a little tea pot"),
            json!(1),
            json!(1.1),
        ] {
            let mut event = base_event();
            event["data"]["contexts"] = json!({ "trace": {"trace_id": invalid} });
            let msg = json!([2, "insert", event]);
            let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
            let result = process_message(payload, kafka_meta(), &ProcessorConfig::default());
            assert!(result.is_err(), "expected error for trace_id {invalid:?}");
        }
    }

    #[test]
    fn test_extract_profile_and_replay_id() {
        let mut event = base_event();
        event["data"]["contexts"] = json!({
            "profile": {"profile_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "replay": {"replay_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
        });
        let row = process_one(event);
        assert_eq!(row["profile_id"], "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        assert_eq!(row["replay_id"], "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    }

    #[test]
    fn test_transaction_duration() {
        let row = process_one(base_event());
        assert_eq!(row["transaction_duration"], 0);

        let mut event = base_event();
        event["data"]["start_timestamp"] = json!(1_687_800_000i64);
        event["data"]["timestamp"] = json!(1_687_800_010i64);
        let row = process_one(event);
        assert_eq!(row["transaction_duration"], 10_000);

        let mut event = base_event();
        event["data"]["start_timestamp"] = json!("not valid");
        event["data"]["timestamp"] = json!({"key": "val"});
        let row = process_one(event);
        assert_eq!(row["transaction_duration"], 0);
    }

    #[test]
    fn test_transaction_duration_does_not_overflow() {
        let mut event = base_event();
        event["data"]["start_timestamp"] = json!(0i64);
        event["data"]["timestamp"] = json!(u32::MAX as i64);
        let row = process_one(event);
        assert_eq!(row["transaction_duration"], u32::MAX);
    }

    #[test]
    fn test_extract_optional_occurrence_fields() {
        let mut event = base_event();
        event["occurrence_data"]["subtitle"] = json!("a subtitle");
        event["occurrence_data"]["culprit"] = json!("the culprit");
        event["occurrence_data"]["level"] = json!("info");
        event["occurrence_data"]["resource_id"] = json!("resource-123");
        let row = process_one(event);
        assert_eq!(row["subtitle"], "a subtitle");
        assert_eq!(row["culprit"], "the culprit");
        assert_eq!(row["level"], "info");
        assert_eq!(row["resource_id"], "resource-123");
    }

    #[test]
    fn test_invalid_version_and_type() {
        let event = base_event();

        let msg = json!([1, "insert", event.clone()]);
        let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
        assert!(process_message(payload, kafka_meta(), &ProcessorConfig::default()).is_err());

        let msg = json!([2, "delete", event]);
        let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
        assert!(process_message(payload, kafka_meta(), &ProcessorConfig::default()).is_err());
    }

    #[test]
    fn test_invalid_uuid_errors() {
        let mut event = base_event();
        event["event_id"] = json!("not-a-uuid");
        let msg = json!([2, "insert", event]);
        let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
        assert!(process_message(payload, kafka_meta(), &ProcessorConfig::default()).is_err());
    }

    #[test]
    fn test_trailing_group_state_ignored() {
        let event = base_event();
        let msg = json!([2, "insert", event, {"is_new": false, "queue": "x"}]);
        let payload = KafkaPayload::new(None, None, Some(serde_json::to_vec(&msg).unwrap()));
        let batch = process_message(payload, kafka_meta(), &ProcessorConfig::default()).unwrap();
        assert_eq!(batch.rows.num_rows, 1);
    }

    #[test]
    fn test_absurd_client_timestamp_does_not_panic() {
        let mut event = base_event();
        event["data"]["client_timestamp"] = json!(1e30);
        let _ = process(event);
    }
}
