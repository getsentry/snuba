use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::processors::utils::{default_retention_days, hex_to_u64, DEFAULT_RETENTION_DAYS};
use crate::types::{InsertBatch, KafkaMessageMetadata, RowData};

pub fn process_message(
    payload: KafkaPayload,
    _: KafkaMessageMetadata,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromSpanMessage = serde_json::from_slice(payload_bytes)?;

    if msg._metrics_summary.is_empty() {
        return Ok(InsertBatch {
            ..Default::default()
        });
    }

    let summaries: MetricsSummaries = msg.try_into()?;
    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(summaries.metrics_summaries.len());

    for summary in &summaries.metrics_summaries {
        let serialized = serde_json::to_vec(&summary)?;
        rows.push(serialized);
    }

    Ok(InsertBatch {
        rows: RowData::from_rows(rows),
        origin_timestamp: None,
        sentry_received_timestamp: None,
    })
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

struct MetricsSummaries {
    metrics_summaries: Vec<MetricsSummary>,
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

impl TryFrom<FromSpanMessage> for MetricsSummaries {
    type Error = anyhow::Error;

    fn try_from(from: FromSpanMessage) -> anyhow::Result<MetricsSummaries> {
        let mut metrics_summaries: Vec<MetricsSummary> = Vec::new();

        let end_timestamp_ms = from.start_timestamp_ms + from.duration_ms as u64;
        for (metric_mri, summaries) in from._metrics_summary {
            for summary in &summaries {
                let (tag_keys, tag_values) = summary.tags.clone().into_iter().unzip();

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

        Ok(MetricsSummaries { metrics_summaries })
    }
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
