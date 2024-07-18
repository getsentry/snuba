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

    attr_str_0: HashMap<String, String>,
    attr_str_1: HashMap<String, String>,
    attr_str_2: HashMap<String, String>,
    attr_str_3: HashMap<String, String>,
    attr_str_4: HashMap<String, String>,
    attr_str_5: HashMap<String, String>,
    attr_str_6: HashMap<String, String>,
    attr_str_7: HashMap<String, String>,
    attr_str_8: HashMap<String, String>,
    attr_str_9: HashMap<String, String>,

    attr_num_0: HashMap<String, f64>,
    attr_num_1: HashMap<String, f64>,
    attr_num_2: HashMap<String, f64>,
    attr_num_3: HashMap<String, f64>,
    attr_num_4: HashMap<String, f64>,
    attr_num_5: HashMap<String, f64>,
    attr_num_6: HashMap<String, f64>,
    attr_num_7: HashMap<String, f64>,
    attr_num_8: HashMap<String, f64>,
    attr_num_9: HashMap<String, f64>,

    attr_bool_0: HashMap<String, bool>,
    attr_bool_1: HashMap<String, bool>,
    attr_bool_2: HashMap<String, bool>,
    attr_bool_3: HashMap<String, bool>,
    attr_bool_4: HashMap<String, bool>,
    attr_bool_5: HashMap<String, bool>,
    attr_bool_6: HashMap<String, bool>,
    attr_bool_7: HashMap<String, bool>,
    attr_bool_8: HashMap<String, bool>,
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
            ];

            sentry_tags.iter().chain(tags.iter()).for_each(|(k, v)| {
                attr_str_buckets[(city_hash_64(k.as_bytes()) as usize) % attr_str_buckets.len()]
                    .insert(k.clone(), v.clone());
                ()
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
            ];

            measurements.iter().for_each(|(k, v)| {
                attr_num_buckets[(city_hash_64(k.as_bytes()) as usize) % attr_num_buckets.len()]
                    .insert(k.clone(), v.value);
                ()
            });
        }

        Ok(res)
    }
}
