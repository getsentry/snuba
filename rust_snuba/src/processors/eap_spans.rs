use anyhow::Context;
use chrono::DateTime;
use seq_macro::seq;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashMap;
use uuid::Uuid;

use schemars::JsonSchema;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde_json::Value;

use crate::config::ProcessorConfig;
use crate::processors::spans::FromSpanMessage;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

pub const ATTRS_SHARD_FACTOR: usize = 20;

macro_rules! seq_attrs {
    ($($tt:tt)*) => {
        seq!(N in 0..20 {
            $($tt)*
        });
    }
}

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    if let Some(headers) = payload.headers() {
        if let Some(ingest_in_eap) = headers.get("ingest_in_eap") {
            if ingest_in_eap == b"false" {
                return Ok(InsertBatch::skip());
            }
        }
    }

    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromSpanMessage = serde_json::from_slice(payload_bytes)?;
    let origin_timestamp = DateTime::from_timestamp(msg.received as i64, 0);
    let mut span: EAPSpan = msg.into();

    span.retention_days = Some(max(
        enforce_retention(span.retention_days, &config.env_config),
        30,
    ));

    InsertBatch::from_rows([span], origin_timestamp)
}

seq_attrs! {
#[derive(Debug, Default, Serialize)]
pub(crate) struct AttributeMap {
    #(
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_~N: HashMap<String, String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_~N: HashMap<String, f64>,
    )*
}
}

impl AttributeMap {
    pub fn insert_str(&mut self, k: String, v: String) {
        seq_attrs! {
            let attr_str_buckets = [
                #(
                &mut self.attr_str_~N,
                )*
            ];
        };

        attr_str_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_str_buckets.len()].insert(k, v);
    }

    pub fn insert_num(&mut self, k: String, v: f64) {
        seq_attrs! {
            let attr_num_buckets = [
                #(
                &mut self.attr_num_~N,
                )*
            ];
        }

        attr_num_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_num_buckets.len()].insert(k, v);
    }
}

#[derive(Debug, Default, Serialize, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub(crate) struct PrimaryKey {
    pub organization_id: u64,
    pub _sort_timestamp: u32,
    pub trace_id: Uuid,
    pub span_id: u64,
}

/// the span object for the new "events analytics platform"
#[derive(Debug, Default, Serialize)]
struct EAPSpan {
    #[serde(flatten)]
    primary_key: PrimaryKey,

    project_id: u64,
    service: String, //currently just project ID as a string
    #[serde(default)]
    parent_span_id: u64,
    segment_id: u64,      //aka transaction ID
    segment_name: String, //aka transaction name
    is_segment: bool,     //aka "is transaction"
    start_timestamp: u64,
    end_timestamp: u64,
    duration_micro: u64,
    exclusive_time_micro: u64,
    retention_days: Option<u16>,
    name: String, //aka description

    sampling_factor: f64,
    sampling_weight: u64, //remove eventually
    sign: u8,             // 1 for additions, -1 for deletions - for this worker it should be 1

    #[serde(flatten)]
    attributes: AttributeMap,
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

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct FromPrimaryKey {
    pub organization_id: u64,
    pub start_timestamp: f64,
    pub trace_id: Uuid,
    pub span_id: String,
}

impl From<FromPrimaryKey> for PrimaryKey {
    fn from(from: FromPrimaryKey) -> PrimaryKey {
        PrimaryKey {
            organization_id: from.organization_id,
            _sort_timestamp: from.start_timestamp as u32,
            trace_id: from.trace_id,
            span_id: u64::from_str_radix(&from.span_id, 16).unwrap_or_default(),
        }
    }
}

impl From<FromSpanMessage> for EAPSpan {
    fn from(from: FromSpanMessage) -> EAPSpan {
        let mut res = Self {
            primary_key: FromPrimaryKey {
                organization_id: from.organization_id,
                start_timestamp: from.start_timestamp_ms as f64 / 1000.0,
                trace_id: from.trace_id,
                span_id: from.span_id,
            }
            .into(),
            project_id: from.project_id,
            service: from.project_id.to_string(),
            parent_span_id: from
                .parent_span_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            segment_id: from
                .segment_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            is_segment: from.is_segment,
            start_timestamp: (from.start_timestamp_precise * 1e6) as u64,
            end_timestamp: (from.end_timestamp_precise * 1e6) as u64,
            duration_micro: (from.end_timestamp_precise * 1e6) as u64
                - (from.start_timestamp_precise * 1e6) as u64,
            exclusive_time_micro: (from.exclusive_time_ms * 1e3) as u64,
            retention_days: from.retention_days,
            name: from.description.unwrap_or_default(),

            sampling_weight: 1,
            sampling_factor: 1.,
            sign: 1,

            ..Default::default()
        };

        {
            if let Some(profile_id) = from.profile_id {
                res.attributes.insert_str(
                    "sentry.profile_id".to_owned(),
                    profile_id.as_simple().to_string(),
                );
            }

            if let Some(sentry_tags) = from.sentry_tags {
                for (k, v) in sentry_tags {
                    if k == "transaction" {
                        res.segment_name = v;
                    } else {
                        res.attributes.insert_str(format!("sentry.{k}"), v);
                    }
                }
            }

            if let Some(tags) = from.tags {
                for (k, v) in tags {
                    res.attributes.insert_str(k, v);
                }
            }

            if let Some(measurements) = from.measurements {
                for (k, v) in measurements {
                    match k.as_str() {
                        "client_sample_rate" if v.value > 0.0 => res.sampling_factor *= v.value,
                        "server_sample_rate" if v.value > 0.0 => res.sampling_factor *= v.value,
                        _ => res.attributes.insert_num(k, v.value),
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
                        Value::Number(number) => res
                            .attributes
                            .insert_num(k, number.as_f64().unwrap_or_default()),
                        Value::Bool(true) => res.attributes.insert_num(k, 1.0),
                        Value::Bool(false) => res.attributes.insert_num(k, 0.0),
                        _ => (),
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
        let span: EAPSpan = msg.into();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(span)
        });
    }
}
