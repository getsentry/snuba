use anyhow::Context;
use chrono::DateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use rust_arroyo::backends::kafka::types::KafkaPayload;

use crate::config::ProcessorConfig;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    _: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let from: FromMetricsSummariesMessage = serde_json::from_slice(payload_bytes)?;

    let group: u64 = u64::from_str_radix(&from.group, 16)?;
    let span_id = u64::from_str_radix(&from.span_id, 16)?;
    let segment_id = u64::from_str_radix(&from.segment_id, 16)?;
    let (tag_keys, tag_values) = from
        .tags
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .unzip();

    let origin_timestamp = DateTime::from_timestamp(from.received as i64, 0);
    let row = MetricsSummary {
        count: from.count,
        deleted: 0,
        duration_ms: from.duration_ms,
        end_timestamp: from.end_timestamp,
        group,
        is_segment: if from.is_segment { 1 } else { 0 },
        max: from.max,
        metric_mri: from.mri,
        min: from.min,
        project_id: from.project_id,
        retention_days: enforce_retention(from.retention_days, &config.env_config),
        segment_id,
        span_id,
        sum: from.sum,
        tag_keys,
        tag_values,
        trace_id: from.trace_id,
    };

    InsertBatch::from_rows([row], origin_timestamp)
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FromMetricsSummariesMessage<'a> {
    #[serde(default)]
    count: u64,
    duration_ms: u32,
    end_timestamp: f64,
    group: &'a str,
    is_segment: bool,
    #[serde(default)]
    max: f64,
    #[serde(default)]
    min: f64,
    mri: &'a str,
    project_id: u64,
    received: f64,
    retention_days: Option<u16>,
    segment_id: &'a str,
    span_id: &'a str,
    #[serde(default)]
    sum: f64,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    trace_id: &'a str,
}

#[derive(Debug, Default, Serialize)]
struct MetricsSummary<'a> {
    count: u64,
    deleted: u8,
    duration_ms: u32,
    end_timestamp: f64,
    group: u64,
    is_segment: u8,
    max: f64,
    metric_mri: &'a str,
    min: f64,
    project_id: u64,
    retention_days: u16,
    segment_id: u64,
    span_id: u64,
    sum: f64,
    #[serde(rename(serialize = "tags.key"))]
    tag_keys: Vec<&'a str>,
    #[serde(rename(serialize = "tags.value"))]
    tag_values: Vec<&'a str>,
    trace_id: &'a str,
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::processors::tests::run_schema_type_test;

    use super::*;

    #[test]
    fn test_valid_summary() {
        let summary = br#"{
          "duration_ms": 1000,
          "end_timestamp": 1691105878.72,
          "group": "deadbeefdeadbeef",
          "is_segment": false,
          "mri": "c:sentry.events.outcomes@none",
          "project_id": 1,
          "received": 169110587919.123,
          "retention_days": 90,
          "segment_id": "deadbeefdeadbeef",
          "span_id": "deadbeefdeadbeef",
          "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
          "count": 1,
          "max": 1.0,
          "min": 1.0,
          "sum": 1.0,
          "tags": {
            "category": "error",
            "environment": "unknown",
            "event_type": "error",
            "outcome": "accepted",
            "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
            "topic": "outcomes-billing",
            "transaction": "sentry.tasks.store.save_event"
          }
        }"#;

        let payload = KafkaPayload::new(None, None, Some(summary.to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn schema() {
        run_schema_type_test::<FromMetricsSummariesMessage>("snuba-metrics-summaries", None);
    }
}
