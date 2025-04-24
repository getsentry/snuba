use anyhow::Context;
use chrono::DateTime;
use seq_macro::seq;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde_json::Value;

use crate::config::ProcessorConfig;
use crate::processors::spans::FromSpanMessage;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

macro_rules! seq_attrs {
    ($($tt:tt)*) => {
        seq!(N in 0..40 {
            $($tt)*
        });
    }
}

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromSpanMessage = serde_json::from_slice(payload_bytes)?;
    let origin_timestamp = DateTime::from_timestamp(msg.received as i64, 0);
    let mut span: EAPItemSpan = msg.into();

    span.retention_days = Some(enforce_retention(span.retention_days, &config.env_config));

    InsertBatch::from_rows([span], origin_timestamp)
}

seq_attrs! {
#[derive(Debug, Default, Serialize)]
pub(crate) struct AttributeMap {
    attributes_bool: HashMap<String, bool>,
    attributes_int: HashMap<String, i64>,
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
            let attr_str_buckets = [
                #(
                &mut self.attributes_string_~N,
                )*
            ];
        };

        attr_str_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_str_buckets.len()].insert(k, v);
    }

    pub fn insert_float(&mut self, k: String, v: f64) {
        seq_attrs! {
            let attr_num_buckets = [
                #(
                &mut self.attributes_float_~N,
                )*
            ];
        }

        attr_num_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_num_buckets.len()].insert(k, v);
    }

    pub fn insert_bool(&mut self, k: String, v: bool) {
        // double write as float and bool
        self.insert_float(k.clone(), v as u8 as f64);
        self.attributes_bool.insert(k, v);
    }

    pub fn insert_int(&mut self, k: String, v: i64) {
        // double write as float and int
        self.insert_float(k.clone(), v as f64);
        self.attributes_int.insert(k, v);
    }
}

#[derive(Debug, Default, Serialize, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub(crate) struct PrimaryKey {
    pub organization_id: u64,
    pub project_id: u64,
    pub item_type: u8,
    pub timestamp: u32,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct EAPItemSpan {
    #[serde(flatten)]
    pub(crate) primary_key: PrimaryKey,

    pub(crate) trace_id: Uuid,
    pub(crate) item_id: u128,
    pub(crate) sampling_weight: u64,
    pub(crate) sampling_factor: f64,
    pub(crate) retention_days: Option<u16>,

    #[serde(flatten)]
    pub(crate) attributes: AttributeMap,
}

fn fnv_1a(input: &[u8]) -> u32 {
    const FNV_1A_PRIME: u32 = 16777619;
    const FNV_1A_OFFSET_BASIS: u32 = 2166136261;

    let mut res = FNV_1A_OFFSET_BASIS;
    for byt in input {
        res ^= *byt as u32;
        res = res.wrapping_mul(FNV_1A_PRIME);
    }

    res
}

impl From<FromSpanMessage> for EAPItemSpan {
    fn from(from: FromSpanMessage) -> EAPItemSpan {
        let item_id = u128::from_str_radix(&from.span_id, 16).unwrap_or_else(|_| {
            panic!(
                "Failed to parse span_id into u128. span_id: {}",
                from.span_id
            )
        });
        let mut res = Self {
            primary_key: PrimaryKey {
                organization_id: from.organization_id,
                project_id: from.project_id,
                item_type: 1, // hardcoding "span" as type
                timestamp: (from.start_timestamp_ms / 1000) as u32,
            },
            trace_id: from.trace_id,
            item_id,
            sampling_weight: 1,
            sampling_factor: 1.0,
            retention_days: from.retention_days,

            ..Default::default()
        };

        {
            if let Some(sentry_tags) = from.sentry_tags {
                for (k, v) in sentry_tags {
                    if k == "description" {
                        res.attributes
                            .insert_str("sentry.normalized_description".to_string(), v);
                    } else {
                        res.attributes.insert_str(format!("sentry.{k}"), v);
                    }
                }
            }

            if let Some(tags) = from.tags {
                for (k, v) in tags {
                    if k == "description" {
                        res.attributes
                            .insert_str("sentry.normalized_description".to_string(), v);
                    } else {
                        res.attributes.insert_str(k, v);
                    }
                }
            }

            if let Some(measurements) = from.measurements {
                for (k, v) in measurements {
                    match k.as_str() {
                        "client_sample_rate" if v.value > 0.0 => res.sampling_factor *= v.value,
                        "server_sample_rate" if v.value > 0.0 => res.sampling_factor *= v.value,
                        _ => res.attributes.insert_float(k, v.value),
                    }
                }
            }
            // lower precision to compensate floating point errors
            res.sampling_factor = (res.sampling_factor * 1e9).round() / 1e9;
            res.sampling_weight = (1.0 / res.sampling_factor).round() as u64;

            if let Some(data) = from.data {
                for (k, v) in data {
                    match v {
                        Value::String(string) => res.attributes.insert_str(k, string),
                        Value::Array(array) => res
                            .attributes
                            .insert_str(k, serde_json::to_string(&array).unwrap_or_default()),
                        Value::Object(object) => res
                            .attributes
                            .insert_str(k, serde_json::to_string(&object).unwrap_or_default()),
                        Value::Number(number) => {
                            if number.is_i64() {
                                res.attributes
                                    .insert_int(k, number.as_i64().unwrap_or_default());
                            } else if number.is_u64() {
                                // as_i64() will return None if the u64 is too large to fit in i64
                                res.attributes
                                    .insert_int(k, number.as_i64().unwrap_or_default());
                            } else {
                                res.attributes
                                    .insert_float(k, number.as_f64().unwrap_or_default());
                            }
                        }
                        Value::Bool(b) => {
                            // insert_bool double writes as a bool and float
                            res.attributes.insert_bool(k.clone(), b);
                        }
                        _ => (),
                    }
                }
            }

            if let Some(description) = from.description {
                res.attributes
                    .insert_str("sentry.raw_description".to_string(), description);
            }

            // insert int double writes as float and int
            res.attributes
                .insert_int("sentry.duration_ms".to_string(), from.duration_ms as i64);

            res.attributes.insert_float(
                "sentry.end_timestamp_precise".to_string(),
                from.end_timestamp_precise,
            );

            if let Some(event_id) = from.event_id {
                res.attributes.insert_str(
                    "sentry.event_id".to_string(),
                    event_id.as_simple().to_string(),
                );
            }

            res.attributes.insert_float(
                "sentry.exclusive_time_ms".to_string(),
                from.exclusive_time_ms,
            );

            res.attributes
                .insert_bool("sentry.is_segment".to_string(), from.is_segment);

            if let Some(parent_span_id) = from.parent_span_id {
                res.attributes
                    .insert_str("sentry.parent_span_id".to_string(), parent_span_id);
            }

            if let Some(profile_id) = from.profile_id {
                res.attributes.insert_str(
                    "sentry.profile_id".to_owned(),
                    profile_id.as_simple().to_string(),
                );
            }

            res.attributes
                .insert_float("sentry.received".to_string(), from.received);

            if let Some(segment_id) = from.segment_id {
                res.attributes
                    .insert_str("sentry.segment_id".to_string(), segment_id);
            }

            res.attributes.insert_float(
                "sentry.start_timestamp_precise".to_string(),
                from.start_timestamp_precise,
            );
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;

    const SPAN_KAFKA_MESSAGE: &str = r#"
{
    "description": "/api/0/relays/projectconfigs/",
    "duration_ms": 152,
    "event_id": "d826225de75d42d6b2f01b957d51f18f",
    "exclusive_time_ms": 0.228,
    "is_segment": true,
    "data": {
        "sentry.environment": "development",
        "sentry.release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
        "thread.name": "uWSGIWorker1Core0",
        "thread.id": "8522009600",
        "sentry.segment.name": "/api/0/relays/projectconfigs/",
        "sentry.sdk.name": "sentry.python.django",
        "sentry.sdk.version": "2.7.0",
        "my.float.field": 101.2,
        "my.int.field": 2000,
        "my.neg.field": -100,
        "my.neg.float.field": -101.2,
        "my.true.bool.field": true,
        "my.false.bool.field": false
    },
    "measurements": {
        "num_of_spans": {
            "value": 50.0
        },
        "client_sample_rate": {
            "value": 0.1
        },
        "server_sample_rate": {
            "value": 0.2
        }
    },
    "profile_id": "56c7d1401ea14ad7b4ac86de46baebae",
    "organization_id": 1,
    "origin": "auto.http.django",
    "project_id": 1,
    "received": 1721319572.877828,
    "retention_days": 90,
    "segment_id": "8873a98879faf06d",
    "sentry_tags": {
        "description": "normalized_description",
        "category": "http",
        "environment": "development",
        "op": "http.server",
        "platform": "python",
        "release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
        "sdk.name": "sentry.python.django",
        "sdk.version": "2.7.0",
        "status": "ok",
        "status_code": "200",
        "thread.id": "8522009600",
        "thread.name": "uWSGIWorker1Core0",
        "trace.status": "ok",
        "transaction": "/api/0/relays/projectconfigs/",
        "transaction.method": "POST",
        "transaction.op": "http.server",
        "user": "ip:127.0.0.1"
    },
    "span_id": "8873a98879faf06d",
    "tags": {
        "http.status_code": "200",
        "relay_endpoint_version": "3",
        "relay_id": "88888888-4444-4444-8444-cccccccccccc",
        "relay_no_cache": "False",
        "relay_protocol_version": "3",
        "relay_use_post_or_schedule": "True",
        "relay_use_post_or_schedule_rejected": "version",
        "server_name": "D23CXQ4GK2.local",
        "spans_over_limit": "False"
    },
    "trace_id": "d099bf9ad5a143cf8f83a98081d0ed3b",
    "start_timestamp_ms": 1721319572616,
    "start_timestamp_precise": 1721319572.616648,
    "end_timestamp_precise": 1721319572.768806
}
    "#;

    #[test]
    fn test_fnv_1a() {
        assert_eq!(fnv_1a("test".as_bytes()), 2949673445)
    }

    #[test]
    fn test_valid_span() {
        let payload = KafkaPayload::new(None, None, Some(SPAN_KAFKA_MESSAGE.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn test_serialization() {
        let msg: FromSpanMessage = serde_json::from_slice(SPAN_KAFKA_MESSAGE.as_bytes()).unwrap();
        let item: EAPItemSpan = msg.into();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(item)
        });
    }

    #[test]
    fn test_sentry_description_to_raw_description() {
        let msg: FromSpanMessage = serde_json::from_slice(SPAN_KAFKA_MESSAGE.as_bytes()).unwrap();
        let item: EAPItemSpan = msg.into();

        // Check that the sentry.description tag is written into description
        assert_eq!(
            item.attributes
                .attributes_string_36
                .get("sentry.normalized_description"),
            Some(&"normalized_description".to_string())
        );

        // Check that description is written into raw_description
        assert_eq!(
            item.attributes
                .attributes_string_11
                .get("sentry.raw_description"),
            Some(&"/api/0/relays/projectconfigs/".to_string())
        );
    }
}
