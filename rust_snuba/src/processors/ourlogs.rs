use anyhow::Context;
use chrono::DateTime;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use uuid::Uuid;

use schemars::JsonSchema;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde::de::{MapAccess, Visitor};

use crate::config::ProcessorConfig;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};
use seq_macro::seq;
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct FromLogMessage {
    //required attributes
    organization_id: u64,
    project_id: u64,
    timestamp_nanos: u64,
    observed_timestamp_nanos: u64,
    retention_days: u16,
    body: String,

    //optional attributes
    trace_id: Option<Uuid>,
    span_id: Option<String>, //hex encoded
    attributes: Option<BTreeMap<String, FromAttribute>>,
    severity_text: Option<String>,
    severity_number: Option<u8>,
}

#[derive(Debug, JsonSchema)]

enum FromAttribute {
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

impl<'de> Deserialize<'de> for FromAttribute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FromAttributeVisitor;

        impl<'de> Visitor<'de> for FromAttributeVisitor {
            type Value = FromAttribute;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map with a single key-value pair for FromAttribute")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let key_value = map.next_entry::<String, serde_json::Value>()?;
                if let Some((key, value)) = key_value {
                    match key.as_str() {
                        "string_value" => {
                            let str_val: String =
                                serde_json::from_value(value).map_err(de::Error::custom)?;
                            Ok(FromAttribute::String(str_val))
                        }
                        "int_value" => {
                            let int_val: i64 =
                                serde_json::from_value(value).map_err(de::Error::custom)?;
                            Ok(FromAttribute::Int(int_val))
                        }
                        "double_value" => {
                            let double_val: f64 =
                                serde_json::from_value(value).map_err(de::Error::custom)?;
                            Ok(FromAttribute::Double(double_val))
                        }
                        "bool_value" => {
                            let bool_val: bool =
                                serde_json::from_value(value).map_err(de::Error::custom)?;
                            Ok(FromAttribute::Bool(bool_val))
                        }
                        _ => Err(de::Error::unknown_field(
                            &key,
                            &["string_value", "int_value", "double_value", "bool_value"],
                        )),
                    }
                } else {
                    Err(de::Error::custom("expected a single key-value pair"))
                }
            }
        }

        deserializer.deserialize_map(FromAttributeVisitor)
    }
}

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromLogMessage = serde_json::from_slice(payload_bytes)?;
    let origin_timestamp = DateTime::from_timestamp(
        (msg.observed_timestamp_nanos / 1_000_000_000) as i64,
        (msg.observed_timestamp_nanos % 1_000_000_000) as u32,
    );
    let mut item: EAPItem = msg.into();

    item.retention_days = enforce_retention(Some(item.retention_days), &config.env_config);

    InsertBatch::from_rows([item], origin_timestamp)
}

macro_rules! seq_attrs {
    ($($tt:tt)*) => {
        seq!(N in 0..40 {
            $($tt)*
        });
    }
}

seq_attrs! {
#[derive(Debug, Default, Serialize)]
pub(crate) struct AttributeMap {
    attributes_bool: BTreeMap<String, bool>,
    attributes_int: BTreeMap<String, i64>,

    #(
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_string_~N: HashMap<String, String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_float_~N: HashMap<String, f64>,
    )*
}
}

impl AttributeMap {
    pub fn insert_str(&mut self, k: String, v: String) {
        seq_attrs! {
            let str_buckets = [
                #(
                &mut self.attributes_string_~N,
                )*
            ];
        };

        str_buckets
            [(crate::processors::eap_spans::fnv_1a(k.as_bytes()) as usize) % str_buckets.len()]
        .insert(k, v);
    }

    pub fn insert_float(&mut self, k: String, v: f64) {
        seq_attrs! {
            let num_buckets = [
                #(
                &mut self.attributes_float_~N,
                )*
            ];
        }

        num_buckets
            [(crate::processors::eap_spans::fnv_1a(k.as_bytes()) as usize) % num_buckets.len()]
        .insert(k, v);
    }

    pub fn insert_int(&mut self, k: String, v: i64) {
        self.attributes_int.insert(k, v);
    }

    pub fn insert_bool(&mut self, k: String, v: bool) {
        self.attributes_bool.insert(k, v);
    }
}

#[derive(Debug, Default, Serialize)]
struct EAPItem {
    organization_id: u64,
    project_id: u64,
    item_type: u8,
    timestamp: u64,
    trace_id: Uuid,
    item_id: u128,
    sampling_weight: u64,
    retention_days: u16,
    #[serde(flatten)]
    attributes: AttributeMap,
}

impl From<FromLogMessage> for EAPItem {
    fn from(from: FromLogMessage) -> EAPItem {
        let mut res = Self {
            organization_id: from.organization_id,
            project_id: from.project_id,
            item_type: 3, // TRACE_ITEM_TYPE_LOG
            item_id: u128::from_be_bytes(
                *Uuid::new_v7(uuid::Timestamp::from_unix(
                    uuid::NoContext,
                    from.timestamp_nanos / 1_000_000_000,
                    (from.timestamp_nanos % 1_000_000_000) as u32,
                ))
                .as_bytes(),
            ),
            trace_id: from.trace_id.unwrap_or_default(),
            retention_days: from.retention_days,
            timestamp: from.timestamp_nanos,
            ..Default::default()
        };
        res.attributes.insert_str(
            "sentry.severity_text".to_string(),
            from.severity_text.unwrap_or_else(|| "INFO".into()),
        );
        res.attributes.insert_int(
            "sentry.severity_number".to_string(),
            from.severity_number.unwrap_or_default().into(),
        );
        res.attributes
            .insert_str("sentry.body".to_string(), from.body);
        if let Some(span_id) = from.span_id {
            res.attributes
                .insert_str("sentry.span_id".to_string(), span_id)
        }

        if let Some(attributes) = from.attributes {
            for (k, v) in attributes {
                match v {
                    FromAttribute::String(s) => {
                        res.attributes.insert_str(k, s);
                    }
                    FromAttribute::Int(i) => {
                        res.attributes.insert_int(k, i);
                    }
                    FromAttribute::Double(d) => {
                        res.attributes.insert_float(k, d);
                    }
                    FromAttribute::Bool(b) => {
                        res.attributes.insert_bool(k, b);
                    }
                }
            }
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;

    const OURLOG_KAFKA_MESSAGE: &str = r#"
{
  "organization_id": 10,
  "project_id": 1,
  "trace_id": "3c8c20d5-0a54-4a1c-ba10-f76f574d856f",
  "trace_flags": 255,
  "span_id": "11002233AABBCCDD",
  "severity_text": "WARNING",
  "severity_number": 1,
  "retention_days": 90,
  "timestamp_nanos": 1715868485371000,
  "observed_timestamp_nanos": 1715868485371000,
  "body": "hello world!",
  "attributes": {
    "some.user.tag": {
      "string_value": "hello"
    },
    "another.user.tag": {
      "int_value": 10
    },
    "double.user.tag": {
      "double_value": -10.59
    },
    "bool.user.tag": {
      "bool_value": true
    }
  }
}
    "#;

    #[test]
    fn test_valid_log() {
        let payload = KafkaPayload::new(None, None, Some(OURLOG_KAFKA_MESSAGE.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }
}
