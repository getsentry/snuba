use adler::Adler32;
use anyhow::Context;
use chrono::DateTime;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, vec};

use crate::{
    types::{InsertBatch, RowData},
    KafkaMessageMetadata, ProcessorConfig,
};

use super::utils::enforce_retention;

const GRANULARITY_TEN_SECONDS: u8 = 0;
const GRANULARITY_ONE_MINUTE: u8 = 1;
const GRANULARITY_ONE_HOUR: u8 = 2;
const GRANULARITY_ONE_DAY: u8 = 3;

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
    tags: &BTreeMap<String, String>,
) -> u32 {
    let mut adler = Adler32::new();

    adler.write_slice(&org_id.to_le_bytes());
    adler.write_slice(&project_id.to_le_bytes());
    adler.write_slice(&metric_id.to_le_bytes());

    for (key, value) in tags {
        adler.write_slice(key.as_bytes());
        adler.write_slice(value.as_bytes());
    }

    adler.checksum()
}

#[derive(Debug, Deserialize)]
struct FromGenericMetricsMessage {
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    #[serde(rename = "type")]
    r#type: String,
    timestamp: u64,
    sentry_received_timestamp: SentryReceivedTimestamp,
    tags: BTreeMap<String, String>,
    value: MetricValue,
    retention_days: u16,
    aggregation_option: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MetricValue {
    Counter(f64),
    SetOrDistribution(Vec<u64>),
    DistributionFloat(Vec<f64>),
    Gauge {
        count: u64,
        last: f64,
        max: f64,
        min: f64,
        sum: f64,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SentryReceivedTimestamp {
    UInt(u64),
    Float(f64),
}

#[derive(Debug, Serialize, Default)]
struct CommonMetricFields {
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: u64,
    retention_days: u16,
    #[serde(rename = "tags.key")]
    tags_key: Vec<u64>,
    #[serde(default, rename = "tags.indexed_value")]
    tags_indexed_value: Vec<u64>,
    #[serde(rename = "tags.raw_value")]
    tags_raw_value: Vec<String>,
    metric_type: String,
    materialization_version: u8,
    timeseries_id: u32,
    granularities: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decasecond_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hr_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    day_retention_days: Option<u8>,
}

/// The raw row that is written to clickhouse for counters.
#[derive(Debug, Serialize, Default)]
struct CountersRawRow {
    #[serde(flatten)]
    common_fields: CommonMetricFields,
    #[serde(default)]
    count_value: f64,
}

/// Parse is the trait which should be implemented for all metric types.
/// It is used to parse the incoming message into the appropriate raw row.
/// Item represents the row into which the message should be parsed.
trait Parse {
    type Item;

    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<Self::Item>>;
}

impl Parse for CountersRawRow {
    type Item = CountersRawRow;

    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<CountersRawRow>> {
        if from.r#type != "c" {
            return Ok(Option::None);
        }

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();
        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        if from.aggregation_option.unwrap_or_default() == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        }
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        let count_value = match from.value {
            MetricValue::Counter(value) => value,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported values provided for counter metric type"
                ))
            }
        };


        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "counter".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 2,
            timeseries_id,
            granularities,
            min_retention_days: Some(retention_days as u8),
            ..Default::default()
        };
        Ok(Some(Self {
            common_fields,
            count_value,
        }))
    }
}

fn process_message<T>(
    payload: KafkaPayload,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch>
where
    T: Parse<Item = T> + Serialize,
{
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: FromGenericMetricsMessage = serde_json::from_slice(payload_bytes)?;

    let sentry_received_timestamp = match msg.sentry_received_timestamp {
        SentryReceivedTimestamp::UInt(timestamp) => {
            DateTime::from_timestamp(timestamp.try_into().unwrap(), 0)
        }
        SentryReceivedTimestamp::Float(timestamp) => DateTime::from_timestamp(timestamp as i64, 0),
    };

    let result: Result<Option<T>, anyhow::Error> = T::parse(msg, config);
    match result {
        Ok(row) => {
            if let Some(row) = row {
                Ok(InsertBatch {
                    rows: RowData::from_rows([row])?,
                    origin_timestamp: None,
                    sentry_received_timestamp,
                })
            } else {
                Ok(InsertBatch::skip())
            }
        }
        Err(err) => Err(err),
    }
}

pub fn process_counter_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    process_message::<CountersRawRow>(payload, config)
}

/// The raw row that is written to clickhouse for sets.
#[derive(Debug, Serialize, Default)]
struct SetsRawRow {
    #[serde(flatten)]
    common_fields: CommonMetricFields,
    set_values: Vec<u64>,
}

impl Parse for SetsRawRow {
    type Item = SetsRawRow;

    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<SetsRawRow>> {
        if from.r#type != "s" {
            return Ok(Option::None);
        }

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();
        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        if from.aggregation_option.unwrap_or_default() == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        }
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        let set_values = match from.value {
            MetricValue::SetOrDistribution(values) => values,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported values provided for gauge metric type"
                ))
            }
        };

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "set".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 2,
            timeseries_id,
            granularities,
            min_retention_days: Some(retention_days as u8),
            ..Default::default()
        };
        Ok(Some(Self {
            common_fields,
            set_values,
        }))
    }
}

pub fn process_set_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    process_message::<SetsRawRow>(payload, config)
}

#[derive(Debug, Serialize, Default)]
struct DistributionsRawRow {
    #[serde(flatten)]
    common_fields: CommonMetricFields,
    distribution_values: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_histogram: Option<u8>,
}

impl Parse for DistributionsRawRow {
    type Item = DistributionsRawRow;

    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<DistributionsRawRow>> {
        if from.r#type != "d" {
            return Ok(Option::None);
        }

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();
        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        let mut enable_histogram = None;
        let aggregate_option = from.aggregation_option.unwrap_or_default();
        if aggregate_option == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        } else if aggregate_option == "hist" {
            enable_histogram = Some(1);
        }
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        let distribution_values = match from.value {
            MetricValue::DistributionFloat(value) => value,
            MetricValue::SetOrDistribution(values) => {
                values.into_iter().map(|v| v as f64).collect()
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported values provided for distribution metric type"
                ))
            }
        };

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "distribution".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 2,
            timeseries_id,
            granularities,
            min_retention_days: Some(retention_days as u8),
            ..Default::default()
        };
        Ok(Some(Self {
            common_fields,
            distribution_values,
            enable_histogram,
        }))
    }
}

pub fn process_distribution_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    process_message::<DistributionsRawRow>(payload, config)
}

#[derive(Debug, Serialize, Default)]
struct GaugesRawRow {
    #[serde(flatten)]
    common_fields: CommonMetricFields,
    #[serde(rename = "gauges_values.last")]
    gauges_values_last: Vec<f64>,
    #[serde(rename = "gauges_values.min")]
    gauges_values_min: Vec<f64>,
    #[serde(rename = "gauges_values.max")]
    gauges_values_max: Vec<f64>,
    #[serde(rename = "gauges_values.sum")]
    gauges_values_sum: Vec<f64>,
    #[serde(rename = "gauges_values.count")]
    gauges_values_count: Vec<u64>,
}

impl Parse for GaugesRawRow {
    type Item = GaugesRawRow;

    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<GaugesRawRow>> {
        if from.r#type != "g" {
            return Ok(Option::None);
        }

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();
        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        let aggregate_option = from.aggregation_option.unwrap_or_default();
        if aggregate_option == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        }
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        let mut gauges_values_last = vec![];
        let mut gauges_values_count = vec![];
        let mut gauges_values_max = vec![];
        let mut gauges_values_min = vec![];
        let mut gauges_values_sum = vec![];
        match from.value {
            MetricValue::Gauge {
                last,
                count,
                max,
                min,
                sum,
            } => {
                gauges_values_last.push(last);
                gauges_values_count.push(count);
                gauges_values_max.push(max);
                gauges_values_min.push(min);
                gauges_values_sum.push(sum);
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported values provided for gauge metric type"
                ))
            }
        }

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "gauge".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 2,
            timeseries_id,
            granularities,
            min_retention_days: Some(retention_days as u8),
            ..Default::default()
        };
        Ok(Some(Self {
            common_fields,
            gauges_values_last,
            gauges_values_count,
            gauges_values_max,
            gauges_values_min,
            gauges_values_sum,
        }))
    }
}

pub fn process_gauge_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    process_message::<GaugesRawRow>(payload, config)
}

#[cfg(test)]
mod tests {
    use crate::processors::ProcessingFunction;

    use super::*;
    use chrono::DateTime;
    use std::time::SystemTime;

    const DUMMY_COUNTER_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65561,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010": "production", "9223372036854776017": "init", "65689": "metric_e2e_spans_counter_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65689":"metric_e2e_spans_counter_k_VUW93LMS"},"d":{"65561":"c:spans/spans@none"}},
        "type": "c",
        "value": 1
    }"#;

    const DUMMY_SET_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65562,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":"production","9223372036854776017":"errored","65690":"metric_e2e_spans_set_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65690":"metric_e2e_spans_set_k_VUW93LMS"},"d":{"65562":"s:spans/error@none"}},
        "type": "s",
        "value": [0, 1, 2, 3, 4, 5]
    }"#;

    const DUMMY_DISTRIBUTION_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65563,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":"production","9223372036854776017":"healthy","65690":"metric_e2e_spans_dist_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"d":{"65560":"d:spans/duration@second"},"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65691":"metric_e2e_spans_dist_k_VUW93LMS"}},
        "type": "d",
        "value": [0, 1, 2, 3, 4, 5]
    }"#;

    const DUMMY_DISTRIBUTION_MESSAGE_WITH_HIST_AGGREGATE_OPTION: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65563,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":"production","9223372036854776017":"healthy","65690":"metric_e2e_spans_dist_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"d":{"65560":"d:spans/duration@second"},"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65691":"metric_e2e_spans_dist_k_VUW93LMS"}},
        "type": "d",
        "value": [0, 1, 2, 3, 4, 5],
        "aggregation_option": "hist"
    }"#;

    const DUMMY_GAUGE_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65564,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940.123,
        "tags": {"9223372036854776010": "production", "9223372036854776017": "init", "65690": "metric_e2e_spans_gauge_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65689":"metric_e2e_spans_gauge_k_VUW93LMS"},"d":{"65564":"g:spans/spans@none"}},
        "type": "g",
        "value": {"count": 10, "last": 10.0, "max": 10.0, "min": 1.0, "sum": 20.0}
    }"#;

    const DUMMY_GAUGE_MESSAGE_WITH_TEN_SECOND_AGGREGATE_OPTION: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65564,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010": "production", "9223372036854776017": "init", "65690": "metric_e2e_spans_gauge_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65689":"metric_e2e_spans_gauge_k_VUW93LMS"},"d":{"65564":"g:spans/spans@none"}},
        "type": "g",
        "value": {"count": 10, "last": 10.0, "max": 10.0, "min": 1.0, "sum": 20.0},
        "aggregation_option": "ten_second"
    }"#;

    #[test]
    fn test_validate_timeseries_id() {
        let org_id = 1;
        let project_id = 2;
        let metric_id = 3;
        let mut tags = BTreeMap::new();
        tags.insert("3".to_string(), "value3".to_string());
        tags.insert("2".to_string(), "value2".to_string());
        tags.insert("1".to_string(), "value1".to_string());

        let timeseries_id = generate_timeseries_id(org_id, project_id, metric_id, &tags);
        assert_eq!(timeseries_id, 1403651978);
    }

    #[cfg(test)]
    fn test_processor_with_payload(
        f: &ProcessingFunction,
        message: &str,
    ) -> Result<InsertBatch, anyhow::Error> {
        let payload = KafkaPayload::new(None, None, Some(message.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let result = f(payload, meta, &ProcessorConfig::default());
        assert!(result.is_ok());
        result
    }

    #[test]
    fn test_counter_processor_with_counter_message() {
        let result = test_processor_with_payload(
            &(process_counter_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_COUNTER_MESSAGE,
        );
        let expected_row = CountersRawRow {
            common_fields: CommonMetricFields {
                use_case_id: "spans".to_string(),
                org_id: 1,
                project_id: 3,
                metric_id: 65561,
                timestamp: 1704614940,
                retention_days: 90,
                tags_key: vec![65689, 9223372036854776010, 9223372036854776017],
                tags_indexed_value: vec![0; 3],
                tags_raw_value: vec![
                    "metric_e2e_spans_counter_v_VUW93LMS".to_string(),
                    "production".to_string(),
                    "init".to_string(),
                ],
                metric_type: "counter".to_string(),
                materialization_version: 2,
                timeseries_id: 1979522105,
                granularities: vec![
                    GRANULARITY_ONE_MINUTE,
                    GRANULARITY_ONE_HOUR,
                    GRANULARITY_ONE_DAY,
                ],
                decasecond_retention_days: None,
                min_retention_days: Some(90),
                hr_retention_days: None,
                day_retention_days: None,
            },
            count_value: 1.0,
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                origin_timestamp: None,
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
            }
        );
    }

    #[test]
    fn test_counter_processor_with_set_message() {
        let result = test_processor_with_payload(
            &(process_counter_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_SET_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::default());
    }

    #[test]
    fn test_set_processor_with_set_message() {
        let result = test_processor_with_payload(
            &(process_set_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_SET_MESSAGE,
        );
        let expected_row = SetsRawRow {
            common_fields: CommonMetricFields {
                use_case_id: "spans".to_string(),
                org_id: 1,
                project_id: 3,
                metric_id: 65562,
                timestamp: 1704614940,
                retention_days: 90,
                tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
                tags_indexed_value: vec![0; 3],
                tags_raw_value: vec![
                    "metric_e2e_spans_set_v_VUW93LMS".to_string(),
                    "production".to_string(),
                    "errored".to_string(),
                ],
                metric_type: "set".to_string(),
                materialization_version: 2,
                timeseries_id: 828906429,
                granularities: vec![
                    GRANULARITY_ONE_MINUTE,
                    GRANULARITY_ONE_HOUR,
                    GRANULARITY_ONE_DAY,
                ],
                decasecond_retention_days: None,
                min_retention_days: Some(90),
                hr_retention_days: None,
                day_retention_days: None,
            },
            set_values: vec![0, 1, 2, 3, 4, 5],
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                origin_timestamp: None,
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
            }
        );
    }

    #[test]
    fn test_set_processor_with_distribution_message() {
        let result = test_processor_with_payload(
            &(process_counter_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_DISTRIBUTION_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::skip());
    }

    #[test]
    fn test_distribution_processor_with_distribution_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_DISTRIBUTION_MESSAGE,
        );
        let expected_row = DistributionsRawRow {
            common_fields: CommonMetricFields {
                use_case_id: "spans".to_string(),
                org_id: 1,
                project_id: 3,
                metric_id: 65563,
                timestamp: 1704614940,
                retention_days: 90,
                tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
                tags_indexed_value: vec![0; 3],
                tags_raw_value: vec![
                    "metric_e2e_spans_dist_v_VUW93LMS".to_string(),
                    "production".to_string(),
                    "healthy".to_string(),
                ],
                metric_type: "distribution".to_string(),
                materialization_version: 2,
                timeseries_id: 1436359714,
                granularities: vec![
                    GRANULARITY_ONE_MINUTE,
                    GRANULARITY_ONE_HOUR,
                    GRANULARITY_ONE_DAY,
                ],
                decasecond_retention_days: None,
                min_retention_days: Some(90),
                hr_retention_days: None,
                day_retention_days: None,
            },
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            enable_histogram: None,
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                origin_timestamp: None,
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
            }
        );
    }

    #[test]
    fn test_distribution_aggregate_option() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_DISTRIBUTION_MESSAGE_WITH_HIST_AGGREGATE_OPTION,
        );
        let expected_row = DistributionsRawRow {
            common_fields: CommonMetricFields {
                use_case_id: "spans".to_string(),
                org_id: 1,
                project_id: 3,
                metric_id: 65563,
                timestamp: 1704614940,
                retention_days: 90,
                tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
                tags_indexed_value: vec![0; 3],
                tags_raw_value: vec![
                    "metric_e2e_spans_dist_v_VUW93LMS".to_string(),
                    "production".to_string(),
                    "healthy".to_string(),
                ],
                metric_type: "distribution".to_string(),
                materialization_version: 2,
                timeseries_id: 1436359714,
                granularities: vec![
                    GRANULARITY_ONE_MINUTE,
                    GRANULARITY_ONE_HOUR,
                    GRANULARITY_ONE_DAY,
                ],
                decasecond_retention_days: None,
                min_retention_days: Some(90),
                hr_retention_days: None,
                day_retention_days: None,
            },
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            enable_histogram: Some(1),
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                origin_timestamp: None,
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
            }
        );
    }

    #[test]
    fn test_distribution_processor_with_gauge_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_GAUGE_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::skip());
    }

    #[test]
    fn test_gauge_processor_with_gauge_message() {
        let result = test_processor_with_payload(
            &(process_gauge_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_GAUGE_MESSAGE,
        );
        let expected_row = GaugesRawRow {
            common_fields: CommonMetricFields {
                use_case_id: "spans".to_string(),
                org_id: 1,
                project_id: 3,
                metric_id: 65564,
                timestamp: 1704614940,
                retention_days: 90,
                tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
                tags_indexed_value: vec![0; 3],
                tags_raw_value: vec![
                    "metric_e2e_spans_gauge_v_VUW93LMS".to_string(),
                    "production".to_string(),
                    "init".to_string(),
                ],
                metric_type: "gauge".to_string(),
                materialization_version: 2,
                timeseries_id: 569776957,
                granularities: vec![
                    GRANULARITY_ONE_MINUTE,
                    GRANULARITY_ONE_HOUR,
                    GRANULARITY_ONE_DAY,
                ],
                decasecond_retention_days: None,
                min_retention_days: Some(90),
                hr_retention_days: None,
                day_retention_days: None,
            },
            gauges_values_last: vec![10.0],
            gauges_values_count: vec![10],
            gauges_values_max: vec![10.0],
            gauges_values_min: vec![1.0],
            gauges_values_sum: vec![20.0],
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                origin_timestamp: None,
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
            }
        );
    }

    #[test]
    fn test_gauge_processor_with_aggregate_option() {
        let result = test_processor_with_payload(
            &(process_gauge_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_GAUGE_MESSAGE_WITH_TEN_SECOND_AGGREGATE_OPTION,
        );
        let expected_row = GaugesRawRow {
            common_fields: CommonMetricFields {
                use_case_id: "spans".to_string(),
                org_id: 1,
                project_id: 3,
                metric_id: 65564,
                timestamp: 1704614940,
                retention_days: 90,
                tags_key: vec![65690, 9223372036854776010, 9223372036854776017],
                tags_indexed_value: vec![0; 3],
                tags_raw_value: vec![
                    "metric_e2e_spans_gauge_v_VUW93LMS".to_string(),
                    "production".to_string(),
                    "init".to_string(),
                ],
                metric_type: "gauge".to_string(),
                materialization_version: 2,
                timeseries_id: 569776957,
                granularities: vec![
                    GRANULARITY_ONE_MINUTE,
                    GRANULARITY_ONE_HOUR,
                    GRANULARITY_ONE_DAY,
                    GRANULARITY_TEN_SECONDS,
                ],
                decasecond_retention_days: None,
                min_retention_days: Some(90),
                hr_retention_days: None,
                day_retention_days: None,
            },
            gauges_values_last: vec![10.0],
            gauges_values_count: vec![10],
            gauges_values_max: vec![10.0],
            gauges_values_min: vec![1.0],
            gauges_values_sum: vec![20.0],
        };
        assert_eq!(
            result.unwrap(),
            InsertBatch {
                rows: RowData::from_rows([expected_row]).unwrap(),
                origin_timestamp: None,
                sentry_received_timestamp: DateTime::from_timestamp(1704614940, 0),
            }
        );
    }

    #[test]
    fn test_gauge_processor_with_counter_message() {
        let result = test_processor_with_payload(
            &(process_gauge_message
                as fn(
                    rust_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_COUNTER_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::skip());
    }
}
