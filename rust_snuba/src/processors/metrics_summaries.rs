use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::processors::utils::{default_retention_days, hex_to_u64, DEFAULT_RETENTION_DAYS};
use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    _: KafkaMessageMetadata,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let from: InputMessage = serde_json::from_slice(payload_bytes)?;

    let mut metrics_summaries: Vec<MetricsSummary> = Vec::new();

    let end_timestamp_ms = from.start_timestamp_ms + from.duration_ms as u64;
    for (metric_mri, summaries) in &from._metrics_summary {
        for summary in summaries {
            let (tag_keys, tag_values) = summary
                .tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .unzip();

            metrics_summaries.push(MetricsSummary {
                count: summary.count,
                deleted: 0,
                end_timestamp: end_timestamp_ms / 1000,
                max: summary.max,
                metric_mri,
                min: summary.min,
                project_id: from.project_id,
                retention_days: from.retention_days.unwrap_or(DEFAULT_RETENTION_DAYS),
                span_id: from.span_id,
                sum: summary.sum,
                tag_keys,
                tag_values,
                trace_id: from.trace_id,
            })
        }
    }

    InsertBatch::from_rows(metrics_summaries)
}

#[derive(Debug, Default, Deserialize)]
struct InputMessage {
    #[serde(default)]
    _metrics_summary: BTreeMap<String, Vec<FromMetricsSummary>>,
    #[serde(default)]
    duration_ms: u32,
    project_id: u64,
    #[serde(default = "default_retention_days")]
    retention_days: Option<u16>,
    #[serde(deserialize_with = "hex_to_u64")]
    span_id: u64,
    start_timestamp_ms: u64,
    trace_id: Uuid,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct FromMetricsSummary {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
    #[serde(default)]
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Default, Serialize)]
struct MetricsSummary<'a> {
    count: u64,
    deleted: u8,
    end_timestamp: u64,
    max: f64,
    metric_mri: &'a str,
    min: f64,
    project_id: u64,
    retention_days: u16,
    span_id: u64,
    sum: f64,
    #[serde(rename(serialize = "tags.key"))]
    tag_keys: Vec<&'a str>,
    #[serde(rename(serialize = "tags.value"))]
    tag_values: Vec<&'a str>,
    trace_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use std::time::SystemTime;

    #[test]
    fn test_valid_span() {
        let span = br#"{
          "_metrics_summary": {
            "c:sentry.events.outcomes@none": [
              {
                "min": 1.0,
                "max": 1.0,
                "sum": 1.0,
                "count": 1,
                "tags": {
                  "category": "error",
                  "environment": "unknown",
                  "event_type": "error",
                  "outcome": "accepted",
                  "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
                  "topic": "outcomes-billing",
                  "transaction": "sentry.tasks.store.save_event"
                }
              }
            ],
            "c:sentry.events.post_save.normalize.errors@none": [
              {
                "min": 0.0,
                "max": 0.0,
                "sum": 0.0,
                "count": 1,
                "tags": {
                  "environment": "unknown",
                  "event_type": "error",
                  "from_relay": "False",
                  "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
                  "transaction": "sentry.tasks.store.save_event"
                }
              }
            ]
          },
          "duration_ms": 1000,
          "project_id": 1,
          "retention_days": 90,
          "span_id": "deadbeefdeadbeef",
          "start_timestamp_ms": 1691105878720,
          "trace_id": "1f32eb1d-9caf-49ac-a21f-6823486cf581"
        }"#;

        let payload = KafkaPayload::new(None, None, Some(span.to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
