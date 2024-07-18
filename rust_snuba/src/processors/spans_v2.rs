use anyhow::Context;
use chrono::DateTime;
use cityhash::cityhash_1::city_hash_64;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use rust_arroyo::backends::kafka::types::KafkaPayload;

use crate::config::ProcessorConfig;
use crate::processors::spans::FromSpanMessage;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromSpanMessage = serde_json::from_slice(payload_bytes)?;

    let origin_timestamp = DateTime::from_timestamp(msg.received as i64, 0);
    let mut span: SpanV2 = msg.try_into()?;

    span.retention_days = Some(enforce_retention(span.retention_days, &config.env_config));

    InsertBatch::from_rows([span], origin_timestamp)
}

#[derive(Debug, Default, Serialize)]
struct SpanV2 {
    organization_id: u64,
    project_id: u64,
    trace_id: Uuid,
    span_id: u64,
    #[serde(default)]
    parent_span_id: u64,
    segment_id: u64,      //aka transaction ID
    segment_name: String, //aka transaction name
    is_segment: bool,     //aka "is transaction"
    _sort_timestamp: u32,
    start_timestamp: u64,
    end_timestamp: u64,
    duration_ms: u32,
    exclusive_time_ms: f64,
    retention_days: Option<u16>,
    name: String, //aka description

    sampling_factor: f64,
    sampling_weight: f64,
    sign: u8, //1 for additions, -1 for deletions - for this worker it should be 1

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_0: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_1: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_2: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_3: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_4: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_5: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_6: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_7: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_8: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_9: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_10: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_11: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_12: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_13: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_14: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_15: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_16: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_17: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_18: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_19: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_20: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_21: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_22: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_23: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_24: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_25: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_26: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_27: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_28: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_29: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_30: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_31: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_32: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_33: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_34: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_35: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_36: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_37: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_38: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_39: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_40: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_41: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_42: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_43: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_44: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_45: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_46: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_47: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_48: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_49: HashMap<String, String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_0: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_1: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_2: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_3: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_4: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_5: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_6: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_7: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_8: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_9: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_10: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_11: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_12: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_13: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_14: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_15: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_16: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_17: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_18: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_19: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_20: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_21: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_22: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_23: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_24: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_25: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_26: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_27: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_28: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_29: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_30: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_31: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_32: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_33: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_34: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_35: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_36: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_37: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_38: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_39: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_40: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_41: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_42: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_43: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_44: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_45: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_46: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_47: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_48: HashMap<String, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_49: HashMap<String, f64>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_0: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_1: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_2: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_3: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_4: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_5: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_6: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_7: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_8: HashMap<String, bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_bool_9: HashMap<String, bool>,
}

impl TryFrom<FromSpanMessage> for SpanV2 {
    type Error = anyhow::Error;

    fn try_from(from: FromSpanMessage) -> anyhow::Result<SpanV2> {
        let sentry_tags = from.sentry_tags.unwrap_or_default();
        let tags = from.tags.unwrap_or_default();
        let measurements = from.measurements.unwrap_or_default();

        let mut res = Self {
            organization_id: from.organization_id,
            project_id: from.project_id,
            trace_id: from.trace_id,
            span_id: u64::from_str_radix(&from.span_id, 16).unwrap_or_default(),
            parent_span_id: from
                .parent_span_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            segment_id: from
                .segment_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            segment_name: sentry_tags.get("transaction").cloned().unwrap_or_default(),
            is_segment: from.is_segment,
            _sort_timestamp: (from.start_timestamp_ms / 1000) as u32,
            start_timestamp: from.start_timestamp_ms,
            end_timestamp: from.start_timestamp_ms + from.duration_ms as u64,
            duration_ms: from.duration_ms,
            exclusive_time_ms: from.exclusive_time_ms,
            retention_days: from.retention_days,
            name: from.description.unwrap_or_default(),

            sampling_weight: 1.,
            sampling_factor: 1.,
            sign: 1,

            ..Default::default()
        };

        {
            let mut attr_str_buckets = [
                &mut res.attr_str_0,
                &mut res.attr_str_1,
                &mut res.attr_str_2,
                &mut res.attr_str_3,
                &mut res.attr_str_4,
                &mut res.attr_str_5,
                &mut res.attr_str_6,
                &mut res.attr_str_7,
                &mut res.attr_str_8,
                &mut res.attr_str_9,
                &mut res.attr_str_10,
                &mut res.attr_str_11,
                &mut res.attr_str_12,
                &mut res.attr_str_13,
                &mut res.attr_str_14,
                &mut res.attr_str_15,
                &mut res.attr_str_16,
                &mut res.attr_str_17,
                &mut res.attr_str_18,
                &mut res.attr_str_19,
                &mut res.attr_str_20,
                &mut res.attr_str_21,
                &mut res.attr_str_22,
                &mut res.attr_str_23,
                &mut res.attr_str_24,
                &mut res.attr_str_25,
                &mut res.attr_str_26,
                &mut res.attr_str_27,
                &mut res.attr_str_28,
                &mut res.attr_str_29,
                &mut res.attr_str_30,
                &mut res.attr_str_31,
                &mut res.attr_str_32,
                &mut res.attr_str_33,
                &mut res.attr_str_34,
                &mut res.attr_str_35,
                &mut res.attr_str_36,
                &mut res.attr_str_37,
                &mut res.attr_str_38,
                &mut res.attr_str_39,
                &mut res.attr_str_40,
                &mut res.attr_str_41,
                &mut res.attr_str_42,
                &mut res.attr_str_43,
                &mut res.attr_str_44,
                &mut res.attr_str_45,
                &mut res.attr_str_46,
                &mut res.attr_str_47,
                &mut res.attr_str_48,
                &mut res.attr_str_49,
            ];

            sentry_tags.iter().chain(tags.iter()).for_each(|(k, v)| {
                attr_str_buckets[(city_hash_64(k.as_bytes()) as usize) % attr_str_buckets.len()]
                    .insert(k.clone(), v.clone());
            });
        }

        {
            let mut attr_num_buckets = [
                &mut res.attr_num_0,
                &mut res.attr_num_1,
                &mut res.attr_num_2,
                &mut res.attr_num_3,
                &mut res.attr_num_4,
                &mut res.attr_num_5,
                &mut res.attr_num_6,
                &mut res.attr_num_7,
                &mut res.attr_num_8,
                &mut res.attr_num_9,
                &mut res.attr_num_10,
                &mut res.attr_num_11,
                &mut res.attr_num_12,
                &mut res.attr_num_13,
                &mut res.attr_num_14,
                &mut res.attr_num_15,
                &mut res.attr_num_16,
                &mut res.attr_num_17,
                &mut res.attr_num_18,
                &mut res.attr_num_19,
                &mut res.attr_num_20,
                &mut res.attr_num_21,
                &mut res.attr_num_22,
                &mut res.attr_num_23,
                &mut res.attr_num_24,
                &mut res.attr_num_25,
                &mut res.attr_num_26,
                &mut res.attr_num_27,
                &mut res.attr_num_28,
                &mut res.attr_num_29,
                &mut res.attr_num_30,
                &mut res.attr_num_31,
                &mut res.attr_num_32,
                &mut res.attr_num_33,
                &mut res.attr_num_34,
                &mut res.attr_num_35,
                &mut res.attr_num_36,
                &mut res.attr_num_37,
                &mut res.attr_num_38,
                &mut res.attr_num_39,
                &mut res.attr_num_40,
                &mut res.attr_num_41,
                &mut res.attr_num_42,
                &mut res.attr_num_43,
                &mut res.attr_num_44,
                &mut res.attr_num_45,
                &mut res.attr_num_46,
                &mut res.attr_num_47,
                &mut res.attr_num_48,
                &mut res.attr_num_49,
            ];

            measurements.iter().for_each(|(k, v)| {
                attr_num_buckets[(city_hash_64(k.as_bytes()) as usize) % attr_num_buckets.len()]
                    .insert(k.clone(), v.value);
            });
        }

        Ok(res)
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
        "sentry.sdk.version": "2.7.0"
    },
    "measurements": {
        "num_of_spans": {
            "value": 50.0
        }
    },
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
        let span: SpanV2 = msg.try_into().unwrap();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(span)
        });
    }
}
