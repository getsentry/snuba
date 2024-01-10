use crate::config::ProcessorConfig;
use anyhow::Context;
use chrono::DateTime;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::processors::utils::{enforce_retention, hex_to_u64};
use crate::types::{InsertBatch, KafkaMessageMetadata, RowData};

pub fn process_message(
    payload: KafkaPayload,
    _: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: InputMessage = serde_json::from_slice(payload_bytes)?;

    if msg._metrics_summary.is_none() {
        return Ok(InsertBatch {
            ..Default::default()
        });
    }

    let span: FromSpanMessage = serde_json::from_slice(payload_bytes)?;
    let mut metrics_summaries: Vec<MetricsSummary> = Vec::new();
    let end_timestamp_ms = span.start_timestamp_ms + span.duration_ms as u64;
    let retention_days = enforce_retention(span.retention_days, &config.env_config);

    for (metric_mri, summaries) in &span.metrics_summary {
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
                project_id: span.project_id,
                retention_days,
                span_id: span.span_id,
                sum: summary.sum,
                tag_keys,
                tag_values,
                trace_id: span.trace_id,
            })
        }
    }

    let origin_timestamp = DateTime::from_timestamp(span.received as i64, 0);

    Ok(InsertBatch {
        rows: RowData::from_rows(metrics_summaries)?,
        origin_timestamp,
        sentry_received_timestamp: None,
    })
}

#[derive(Debug, Default, Deserialize)]
struct InputMessage {
    _metrics_summary: Option<Box<RawValue>>,
}

#[derive(Debug, Default, Deserialize)]
struct FromSpanMessage {
    #[serde(default)]
    duration_ms: u32,
    #[serde(rename = "_metrics_summary")]
    metrics_summary: BTreeMap<String, Vec<FromMetricsSummary>>,
    project_id: u64,
    received: f64,
    #[serde(default)]
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
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }
}
