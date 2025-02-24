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
    let mut ourlog: Ourlog = msg.into();

    ourlog.retention_days = enforce_retention(Some(ourlog.retention_days), &config.env_config);

    InsertBatch::from_rows([ourlog], origin_timestamp)
}

#[derive(Debug, Default, Serialize)]
struct Ourlog {
    organization_id: u64,
    project_id: u64,
    trace_id: Uuid,
    span_id: u64,
    severity_text: String,
    severity_number: u8,
    retention_days: u16,
    timestamp: u64,
    body: String,
    attr_string: BTreeMap<String, String>,
    attr_int: BTreeMap<String, i64>,
    attr_double: BTreeMap<String, f64>,
    attr_bool: BTreeMap<String, bool>,
}

impl From<FromLogMessage> for Ourlog {
    fn from(from: FromLogMessage) -> Ourlog {
        let mut res = Self {
            organization_id: from.organization_id,
            project_id: from.project_id,
            trace_id: from.trace_id.unwrap_or_default(),
            span_id: from
                .span_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            severity_text: from.severity_text.unwrap_or_else(|| "INFO".into()),
            severity_number: from.severity_number.unwrap_or_default(),
            retention_days: from.retention_days,
            timestamp: from.timestamp_nanos,
            body: from.body,
            attr_string: BTreeMap::new(),
            attr_int: BTreeMap::new(),
            attr_double: BTreeMap::new(),
            attr_bool: BTreeMap::new(),
        };

        if let Some(attributes) = from.attributes {
            for (k, v) in attributes {
                match v {
                    FromAttribute::String(s) => {
                        res.attr_string.insert(k, s);
                    }
                    FromAttribute::Int(i) => {
                        res.attr_int.insert(k, i);
                    }
                    FromAttribute::Double(d) => {
                        res.attr_double.insert(k, d);
                    }
                    FromAttribute::Bool(b) => {
                        res.attr_bool.insert(k, b);
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
