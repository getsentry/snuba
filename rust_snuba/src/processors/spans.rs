use crate::types::{BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::InvalidMessage;
use serde::{Deserialize, Deserializer};
use serde_json::json;
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct SpanMessage {
    #[serde(default, deserialize_with = "deserialize_optional_uuid")]
    event_id: Option<String>,
    #[serde(default)]
    organization_id: Option<u64>,
    project_id: u64,
    #[serde(deserialize_with = "deserialize_uuid")]
    trace_id: String,
    span_id: String,
    #[serde(default)]
    parent_span_id: Option<String>,
    #[serde(default)]
    segment_id: Option<String>,
    #[serde(default)]
    group_raw: Option<String>,
    is_segment: bool,
    start_timestamp_ms: u64,
    duration_ms: u64,
    exclusive_time_ms: u64,
    retention_days: Option<u32>,
    tags: BTreeMap<String, Value>,
    sentry_tags: SentryExtractedTags,
    #[serde(default)]
    description: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct SentryExtractedTags {
    #[serde(default, rename(serialize = "http.method"))]
    http_method: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    system: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    status_code: Option<i32>,
    #[serde(default)]
    transaction: Option<String>,
    #[serde(default, rename(serialize = "transaction.op"))]
    transaction_op: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default, rename(serialize = "transaction.method"))]
    transaction_method: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

fn deserialize_optional_uuid<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    if let Some(s) = s {
        return Ok(Some(
            Uuid::parse_str(&s)
                .map_err(serde::de::Error::custom)?
                .to_string(),
        ));
    }
    Ok(None)
}

fn deserialize_uuid<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    Ok(Uuid::parse_str(&buf)
        .map_err(serde::de::Error::custom)?
        .to_string())
}

fn enforce_retention(_: &SpanMessage) -> Option<u32> {
    Some(90)
}

fn process_span_meta(event: &SpanMessage, processed: &mut BTreeMap<&str, Value>) {
    processed.insert("trace_id", json!(event.trace_id));
    processed.insert(
        "span_id",
        json!(u128::from_str_radix(event.span_id.as_str(), 16).unwrap()),
    );
    processed.insert("segment_id", processed.get("span_id").unwrap().clone());
    processed.insert("is_segment", json!(event.is_segment));
    processed.insert(
        "parent_span_id",
        json!(event.parent_span_id.as_ref().map_or(None, |id| Some(
            u128::from_str_radix(id.as_str(), 16).unwrap()
        ))),
    );
    processed.insert("transaction_id", json!(event.event_id));

    processed.insert(
        "description",
        json!(event.description.as_ref().map_or("", |desc| desc.as_str())),
    );
    processed.insert(
        "group_raw",
        json!(event
            .group_raw
            .as_ref()
            .map_or(0, |group_raw| u64::from_str_radix(group_raw, 16).unwrap())),
    );
}

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> Result<BytesInsertBatch, InvalidMessage> {
    if let Some(payload_bytes) = payload.payload {
        let event: SpanMessage = serde_json::from_slice(&payload_bytes).map_err(|err| {
            let s = err.to_string();
            log::error!("Failed to deserialize message: {}, {}", err, s);
            InvalidMessage
        })?;
        let retention_days = enforce_retention(&event).ok_or_else(|| {
            log::error!("Failed to process message: event too old");
            InvalidMessage
        })?;

        let mut processed: BTreeMap<&str, Value> = BTreeMap::from([
            ("deleted", json!(0)),
            ("retention_days", json!(retention_days)),
            ("partition", json!(_metadata.partition)),
            ("offset", json!(_metadata.offset)),
            ("project_id", json!(event.project_id)),
        ]);

        process_span_meta(&event, &mut processed);

        let serialized = serde_json::to_vec(&processed).map_err(|err| {
            log::error!("Failed to serialize processed message: {}", err);
            InvalidMessage
        })?;

        return Ok(BytesInsertBatch {
            rows: vec![serialized],
        });
    }

    Err(InvalidMessage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

    #[test]
    fn test_spans() {
        let data = r#"
        {
            "event_id": "dcc403b73ef548648188bbfa6012e9dc",
            "organization_id": 69,
            "project_id": 1,
            "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
            "span_id": "deadbeefdeadbeef",
            "parent_span_id": "deadbeefdeadbeef",
            "segment_id": "deadbeefdeadbeef",
            "group_raw": "b640a0ce465fa2a4",
            "is_segment": false,
            "start_timestamp_ms": 1691105878720,
            "duration_ms": 1000,
            "exclusive_time_ms": 1000,
            "retention_days": 90,
            "tags": {
              "tag1": "value1",
              "tag2": 123,
              "tag3": true
            },
            "sentry_tags": {
              "http.method": "GET",
              "action": "GET",
              "domain": "targetdomain.tld:targetport",
              "module": "http",
              "group": "deadbeefdeadbeef",
              "status": "ok",
              "system": "python",
              "status_code": 200,
              "transaction": "/organizations/:orgId/issues/",
              "transaction.op": "navigation",
              "op": "http.client",
              "transaction.method": "GET"
            }
          }
        "#;
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
