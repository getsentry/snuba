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
    let from: FromSpanMessage = simd_json::serde::from_slice(payload_bytes.clone().as_mut_slice())?;
    let mut metrics_summaries: Vec<MetricsSummary> =
        Vec::with_capacity(from._metrics_summary.len());

    let end_timestamp_ms = from.start_timestamp_ms + from.duration_ms as u64;
    for (metric_mri, summaries) in from._metrics_summary {
        for summary in summaries {
            let (tag_keys, tag_values) = summary.tags.into_iter().unzip();

            metrics_summaries.push(MetricsSummary {
                count: summary.count,
                deleted: 0,
                end_timestamp: end_timestamp_ms / 1000,
                max: summary.max,
                metric_mri: metric_mri.clone(),
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
struct FromSpanMessage {
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
struct MetricsSummary {
    count: u64,
    deleted: u8,
    end_timestamp: u64,
    max: f64,
    metric_mri: String,
    min: f64,
    project_id: u64,
    retention_days: u16,
    span_id: u64,
    sum: f64,
    #[serde(rename(serialize = "tags.key"))]
    tag_keys: Vec<String>,
    #[serde(rename(serialize = "tags.value"))]
    tag_values: Vec<String>,
    trace_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use std::collections::BTreeMap;
    use std::time::SystemTime;

    #[derive(Debug, Default, Deserialize, Serialize)]
    struct TestSpanMessage {
        _metrics_summary: BTreeMap<String, Vec<FromMetricsSummary>>,
        duration_ms: Option<u32>,
        project_id: Option<u64>,
        retention_days: Option<u16>,
        span_id: Option<String>,
        start_timestamp_ms: Option<u64>,
        trace_id: Option<Uuid>,
    }

    fn valid_span() -> TestSpanMessage {
        TestSpanMessage {
            _metrics_summary: BTreeMap::new(),
            duration_ms: Some(1000),
            project_id: Some(1),
            retention_days: Some(90),
            span_id: Some("deadbeefdeadbeef".into()),
            start_timestamp_ms: Some(1691105878720),
            trace_id: Some(Uuid::new_v4()),
        }
    }

    #[test]
    fn test_valid_span() {
        let mut span = valid_span();
        span._metrics_summary = BTreeMap::from([
            (
                "c:sentry.events.outcomes@none".into(),
                vec![FromMetricsSummary {
                    count: 1,
                    max: 1.0,
                    min: 1.0,
                    sum: 1.0,
                    tags: BTreeMap::from([
                        ("category".into(), "error".into()),
                        ("environment".into(), "unknown".into()),
                        ("event_type".into(), "error".into()),
                        ("outcome".into(), "accepted".into()),
                        (
                            "release".into(),
                            "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8".into(),
                        ),
                        ("topic".into(), "outcomes-billing".into()),
                        ("transaction".into(), "sentry.tasks.store.save_event".into()),
                    ]),
                }],
            ),
            (
                "c:sentry.events.post_save.normalize.errors@none".into(),
                vec![FromMetricsSummary {
                    count: 1,
                    max: 0.0,
                    min: 0.0,
                    sum: 0.0,
                    tags: BTreeMap::from([
                        ("environment".into(), "unknown".into()),
                        ("event_type".into(), "error".into()),
                        ("from_relay".into(), "False".into()),
                        (
                            "release".into(),
                            "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8".into(),
                        ),
                        ("transaction".into(), "sentry.tasks.store.save_event".into()),
                    ]),
                }],
            ),
        ]);
        let data = serde_json::to_string(&span);
        assert!(data.is_ok());
        let payload = KafkaPayload::new(None, None, Some(data.unwrap().as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
