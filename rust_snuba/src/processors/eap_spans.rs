use anyhow::Context;
use chrono::DateTime;
use seq_macro::seq;
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
    let mut span: EAPSpan = msg.try_into()?;

    span.retention_days = Some(enforce_retention(span.retention_days, &config.env_config));

    InsertBatch::from_rows([span], origin_timestamp)
}

seq!(N in 0..200 {
#[derive(Debug, Default, Serialize)]
struct EAPSpan {
    // the span object for the new "events analytics platform"
    organization_id: u64,
    project_id: u64,
    service: String, //currently just project ID as a string
    trace_id: Uuid,
    span_id: u64,
    #[serde(default)]
    parent_span_id: u64,
    segment_id: u64,      //aka transaction ID
    segment_name: String, //aka transaction name
    is_segment: bool,     //aka "is transaction"
    start_timestamp: u64,
    end_timestamp: u64,
    duration_ms: u32,
    exclusive_time_ms: f64,
    retention_days: Option<u16>,
    name: String, //aka description

    sampling_factor: f64,
    sampling_weight: f64,
    sign: u8, //1 for additions, -1 for deletions - for this worker it should be 1

    #(
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_str_~N: HashMap<String, String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attr_num_~N: HashMap<String, f64>,
    )*
}
});

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

impl From<FromSpanMessage> for EAPSpan {
    fn from(from: FromSpanMessage) -> EAPSpan {
        let sentry_tags = from.sentry_tags.unwrap_or_default();
        let tags = from.tags.unwrap_or_default();
        let measurements = from.measurements.unwrap_or_default();

        let mut res = Self {
            organization_id: from.organization_id,
            project_id: from.project_id,
            trace_id: from.trace_id,
            service: format!("{:x}", from.project_id),
            span_id: u64::from_str_radix(&from.span_id, 16).unwrap_or_default(),
            parent_span_id: from
                .parent_span_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            segment_id: from
                .segment_id
                .map_or(0, |s| u64::from_str_radix(&s, 16).unwrap_or(0)),
            segment_name: sentry_tags.get("transaction").cloned().unwrap_or_default(),
            is_segment: from.is_segment,
            start_timestamp: (from.start_timestamp_precise * 1e6) as u64,
            end_timestamp: (from.end_timestamp_precise * 1e6) as u64,
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
            seq!(N in 0..200 {
            let mut attr_str_buckets = [
                #(
                &mut res.attr_str_~N,
                )*
            ];
            });

            sentry_tags.iter().chain(tags.iter()).for_each(|(k, v)| {
                attr_str_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_str_buckets.len()]
                    .insert(k.clone(), v.clone());
            });
        }

        {
            seq!(N in 0..200 {
            let mut attr_num_buckets = [
                #(
                &mut res.attr_num_~N,
                )*
            ];
                });

            measurements.iter().for_each(|(k, v)| {
                attr_num_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_num_buckets.len()]
                    .insert(k.clone(), v.value);
            });
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
    fn test_half_md5() {
        //select halfMD5('test') == 688887797400064883
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
        let span: EAPSpan = msg.try_into().unwrap();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(span)
        });
    }
}
