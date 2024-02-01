use adler::Adler32;
use anyhow::Context;
use chrono::DateTime;
use core::f64;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    types::{InsertBatch, RowData},
    KafkaMessageMetadata, ProcessorConfig,
};

use super::utils::enforce_retention;

/// Generate a timeseries ID from the given parameters. Timeseries IDs are used to
/// uniquely identify a timeseries in the database. This implemenation is based on
/// the python implementation in order to ensure that the same timeseries ID is
/// generated for the same input.
///
/// Upstream sends the tag keys and values as strings. Which is surprising since the
/// tag keys are indexed so they should be integers. But we have to deal with it.
fn generate_timeseries_id(
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    tags: &BTreeMap<String, u64>,
) -> u32 {
    let mut adler = Adler32::new();

    adler.write_slice(&org_id.to_le_bytes());
    adler.write_slice(&project_id.to_le_bytes());
    adler.write_slice(&metric_id.to_le_bytes());

    for (key, value) in tags {
        adler.write_slice(key.as_bytes());
        adler.write_slice(&value.to_le_bytes());
    }

    adler.checksum()
}

#[derive(Debug, Deserialize)]
struct FromMetricsMessage {
    #[serde(default = "default_use_case_id")]
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: f64,
    sentry_received_timestamp: Option<f64>,
    tags: BTreeMap<String, u64>,
    #[serde(flatten)]
    value: MetricValue,
    retention_days: u16,
}

fn default_use_case_id() -> String {
    "sessions".into()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
enum MetricValue {
    #[serde(rename = "c")]
    Counter(f64),
    #[serde(rename = "s")]
    Set(Vec<u64>),
    #[serde(rename = "d")]
    Distribution(Vec<f64>),
}

/// The raw row that is written to clickhouse.
#[derive(Debug, Serialize, Default)]
struct MetricsRawRow {
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: u32,
    retention_days: u16,
    #[serde(rename = "tags.key")]
    tags_key: Vec<u64>,
    #[serde(default, rename = "tags.value")]
    tags_value: Vec<u64>,
    metric_type: String,
    set_values: Option<Vec<u64>>,
    count_value: Option<f64>,
    distribution_values: Option<Vec<f64>>,
    materialization_version: u8,
    timeseries_id: u32,
    partition: u16,
    offset: u64,
}

/// Parse is the trait which should be implemented for all metric types.
/// It is used to parse the incoming message into the appropriate raw row.
/// Item represents the row into which the message should be parsed.
trait Parse {
    fn parse(
        from: FromMetricsMessage,
        meta: KafkaMessageMetadata,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<MetricsRawRow>>;
}

impl Parse for MetricsRawRow {
    fn parse(
        from: FromMetricsMessage,
        meta: KafkaMessageMetadata,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<MetricsRawRow>> {
        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);

        dbg!(timeseries_id);

        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        Ok(Some(MetricsRawRow {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_id: from.metric_id,
            timestamp: from.timestamp as u32,
            retention_days,
            tags_key: tag_keys.into_iter().map(|e| e.parse().unwrap()).collect(),
            tags_value: tag_values,
            materialization_version: 4,
            timeseries_id,
            partition: meta.partition,
            offset: meta.offset,
            ..match from.value {
                MetricValue::Set(value) => MetricsRawRow {
                    metric_type: "set".to_string(),
                    set_values: Some(value),
                    ..Default::default()
                },
                MetricValue::Counter(value) => MetricsRawRow {
                    metric_type: "counter".to_string(),
                    count_value: Some(value),
                    ..Default::default()
                },
                MetricValue::Distribution(value) => MetricsRawRow {
                    metric_type: "distribution".to_string(),
                    distribution_values: Some(value),
                    ..Default::default()
                },
            }
        }))
    }
}

pub fn process_metrics_message(
    payload: KafkaPayload,
    meta: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg = serde_json::from_slice::<FromMetricsMessage>(payload_bytes)?;

    let sentry_received_timestamp = msg
        .sentry_received_timestamp
        .and_then(|v| DateTime::from_timestamp(v as i64, 0));

    // dbg!(sentry_received_timestamp);

    let result: Result<Option<MetricsRawRow>, anyhow::Error> =
        MetricsRawRow::parse(msg, meta, config);
    match result {
        Ok(row) => {
            if let Some(row) = row {
                Ok(InsertBatch {
                    rows: RowData::from_rows([row])?,
                    origin_timestamp: None,
                    sentry_received_timestamp,
                    cogs_data: None,
                })
            } else {
                Ok(InsertBatch::skip())
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use crate::processors::ProcessingFunction;

    use super::*;
    use chrono::DateTime;
    use std::time::SystemTime;

    #[test]
    fn test_validate_timeseries_id() {
        let org_id = 1;
        let project_id = 2;
        let metric_id = 3;
        let mut tags = BTreeMap::new();
        tags.insert("3".to_string(), 3);
        tags.insert("2".to_string(), 2);
        tags.insert("1".to_string(), 1);

        let timeseries_id = generate_timeseries_id(org_id, project_id, metric_id, &tags);
        assert_eq!(timeseries_id, 200474787);
    }

    const DUMMY_COUNTER_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "sessions",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65561,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010": 1, "9223372036854776017": 2, "65689": 3},
        "retention_days": 90,
        "mapping_meta":{
            "h":{
                "9223372036854776017":"2",
                "9223372036854776010":"1"
            },
            "f":{"65689":"3"},
            "d":{"65561":"c:sessions/sessions@none"}
        },
        "type": "c",
        "value": 1
    }"#;

    const DUMMY_SET_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "sessions",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65562,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":1,"9223372036854776017":2,"65690":3},
        "retention_days": 90,
        "mapping_meta":{"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65690":"metric_e2e_spans_set_k_VUW93LMS"},"d":{"65562":"s:spans/error@none"}},
        "type": "s",
        "value": [0, 1, 2, 3, 4, 5]
    }"#;

    const DUMMY_DISTRIBUTION_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "sessions",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65563,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":1,"9223372036854776017":2,"65690":3},
        "retention_days": 90,
        "mapping_meta":{"d":{"65560":"d:spans/duration@second"},"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65691":"metric_e2e_spans_dist_k_VUW93LMS"}},
        "type": "d",
        "value": [0, 1, 2, 3, 4, 5]
    }"#;

    #[cfg(test)]
    fn test_processor_with_payload(
        f: &ProcessingFunction,
        message: &str,
    ) -> Result<InsertBatch, anyhow::Error> {
        let payload = KafkaPayload::new(None, None, Some(message.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 0,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let result = f(payload, meta, &ProcessorConfig::default());
        assert!(result.is_ok());
        result
    }

    #[test]
    fn test_counter_processor_with_counter_message() {
        let result = test_processor_with_payload(
            &(process_metrics_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_COUNTER_MESSAGE,
        );
        let expected_row = MetricsRawRow {
            use_case_id: "sessions".to_string(),
            org_id: 1,
            project_id: 3,
            metric_id: 65561,
            timestamp: 1704614940,
            retention_days: 90,
            tags_key: vec![65689, 9223372036854776010, 9223372036854776017],
            tags_value: vec![3, 1, 2],
            metric_type: "counter".to_string(),
            count_value: Some(1.0),
            materialization_version: 4,
            timeseries_id: 1129056500,
            ..Default::default()
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_set_processor_with_set_message() {
        let result = test_processor_with_payload(
            &(process_metrics_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_SET_MESSAGE,
        );
        let expected_row = MetricsRawRow {
            use_case_id: "sessions".to_string(),
            org_id: 1,
            project_id: 3,
            metric_id: 65562,
            timestamp: 1704614940,
            retention_days: 90,
            tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
            tags_value: vec![3, 1, 2],
            metric_type: "set".to_string(),
            set_values: vec![0, 1, 2, 3, 4, 5].into(),
            materialization_version: 4,
            timeseries_id: 1101007085,
            ..Default::default()
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dist_processor_with_dist_message() {
        let result = test_processor_with_payload(
            &(process_metrics_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_DISTRIBUTION_MESSAGE,
        );
        let expected_row = MetricsRawRow {
            use_case_id: "sessions".to_string(),
            org_id: 1,
            project_id: 3,
            metric_id: 65563,
            timestamp: 1704614940,
            retention_days: 90,
            tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
            tags_value: vec![3, 1, 2],
            metric_type: "distribution".to_string(),
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0].into(),
            materialization_version: 4,
            timeseries_id: 1105922286,
            ..Default::default()
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
                ..Default::default()
            }
        );
    }
}
