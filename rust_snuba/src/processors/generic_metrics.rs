use adler::Adler32;
use anyhow::Context;
use chrono::DateTime;
use sentry_options::options;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

use crate::{
    types::{CogsData, InsertBatch, RowData},
    KafkaMessageMetadata, ProcessorConfig,
};

use sentry_arroyo::backends::kafka::types::{Headers, KafkaPayload};
use sentry_arroyo::{counter, timer};

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
    tags: &BTreeMap<&str, String>,
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
struct FromGenericMetricsMessage<'a> {
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: f64,
    sentry_received_timestamp: f64,
    #[serde(borrow)]
    tags: BTreeMap<&'a str, String>,
    #[serde(rename = "type")]
    metric_type: MetricType,
    #[serde(borrow)]
    value: &'a RawValue,
    retention_days: u16,
    sampling_weight: Option<f64>,
    aggregation_option: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageUseCase {
    use_case_id: String,
}

/// The metric type as sent in the `type` field of the message. The actual
/// `value` payload is deserialized separately (by type) so that the message
/// struct does not need `#[serde(flatten)]`, which forces serde into a slow,
/// borrow-breaking buffered code path.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
enum MetricType {
    #[serde(rename = "c")]
    Counter,
    #[serde(other)]
    Other,
}

#[derive(Debug, Serialize, Default)]
struct CommonMetricFields {
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: u32,
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
    sampling_weight: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decasecond_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hr_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    day_retention_days: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_meta: Option<u8>,
}

fn should_record_meta(use_case_id: &str) -> Option<u8> {
    match use_case_id {
        "escalating_issues" => Some(0),
        _ => Some(1),
    }
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
trait Parse: Sized {
    fn parse(
        from: FromGenericMetricsMessage<'_>,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<Self>>;
}

impl Parse for CountersRawRow {
    fn parse(
        from: FromGenericMetricsMessage<'_>,
        _config: &ProcessorConfig,
    ) -> anyhow::Result<Option<CountersRawRow>> {
        if from.metric_type != MetricType::Counter {
            return Ok(Option::None);
        }
        let count_value: f64 = serde_json::from_str(from.value.get())?;

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();

        timer!("generic_metrics.messages.tags_len", tag_keys.len() as u64, "metric_type" => "counter");

        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        if from.aggregation_option.unwrap_or_default() == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        }
        let retention_days = enforce_retention(Some(from.retention_days));

        let record_meta = should_record_meta(from.use_case_id.as_str());

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "counter".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp as u32,
            retention_days,
            tags_key: tag_keys
                .iter()
                .map(|k| k.parse::<u64>())
                .collect::<Result<Vec<_>, _>>()?,
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 3,
            sampling_weight: from
                .sampling_weight
                .map(|sampling_weight| sampling_weight as u64),
            timeseries_id,
            granularities,
            min_retention_days: Some(retention_days as u8),
            record_meta,
            ..Default::default()
        };
        Ok(Some(Self {
            common_fields,
            count_value,
        }))
    }
}

#[inline]
fn should_use_killswitch(
    killswitch_config: Option<String>,
    payload_str: &str,
) -> anyhow::Result<bool> {
    if let Some(config) = killswitch_config {
        let use_case = serde_json::from_str::<MessageUseCase>(payload_str)?;
        if config.contains(&use_case.use_case_id) {
            counter!("generic_metrics.messages.killswitched_use_case", 1, "use_case_id" => &use_case.use_case_id);
            return Ok(true);
        }
    }

    Ok(false)
}

fn process_message<T>(
    payload: KafkaPayload,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch>
where
    T: Parse + Serialize,
{
    let payload_bytes = payload.payload().context("Expected payload")?;
    let payload_str = str::from_utf8(payload_bytes)?;

    let killswitch_config = options("snuba")
        .ok()
        .and_then(|o| o.get("generic_metrics_use_case_killswitch").ok())
        .and_then(|v| v.as_str().map(String::from));
    if should_use_killswitch(killswitch_config, payload_str)? {
        return Ok(InsertBatch::skip());
    }

    let msg: FromGenericMetricsMessage = serde_json::from_str(payload_str)?;
    let use_case_id = msg.use_case_id.clone();
    let sentry_received_timestamp =
        DateTime::from_timestamp(msg.sentry_received_timestamp as i64, 0);

    let result: Result<Option<T>, anyhow::Error> = T::parse(msg, config);

    timer!("generic_metrics.messages.size", payload_bytes.len() as f64);

    match result {
        Ok(row) => {
            if let Some(row) = row {
                Ok(InsertBatch {
                    rows: RowData::from_rows([row])?,
                    origin_timestamp: None,
                    sentry_received_timestamp,
                    cogs_data: Some(CogsData {
                        data: BTreeMap::from([(
                            format!("genericmetrics_{use_case_id}"),
                            payload_bytes.len() as u64,
                        )]),
                    }),
                    item_type_metrics: None,
                })
            } else {
                Ok(InsertBatch::skip())
            }
        }
        Err(err) => Err(err),
    }
}

// MetricTypeHeader specifies what type of metric was sent on the message
// as per the kafka headers. It is possible to get data with heades missing
// altogether or the specific header key with which to determine the metric
// type to be missing. Hence there is an Unknown variant
#[derive(Debug, Default, PartialEq, Clone)]
enum MetricTypeHeader {
    #[default]
    Unknown,
    Counter,
    Other,
}

impl MetricTypeHeader {
    fn from_kafka_header(header: Option<&Headers>) -> Self {
        if let Some(headers) = header {
            if let Some(header_value) = headers.get("metric_type") {
                match header_value {
                    b"c" => MetricTypeHeader::Counter,
                    _ => MetricTypeHeader::Other,
                }
            } else {
                // metric_type header not found
                MetricTypeHeader::Unknown
            }
        } else {
            // No headers on message
            MetricTypeHeader::Unknown
        }
    }
}

pub fn process_counter_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let metric_type_header = MetricTypeHeader::from_kafka_header(payload.headers());
    match metric_type_header {
        MetricTypeHeader::Counter | MetricTypeHeader::Unknown => {
            process_message::<CountersRawRow>(payload, config)
        }
        _ => Ok(InsertBatch::skip()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::processors::ProcessingFunction;

    use super::*;
    use chrono::{DateTime, Utc};
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

    const DUMMY_COUNTER_MESSAGE_WITH_SAMPLING_WEIGHT: &str = r#"{
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
        "value": 1,
        "sampling_weight": 100.1
    }"#;

    /// Helper function for tests to create expected InsertBatch.
    /// Since generic_metrics never populates item_type_metrics, this helper
    /// always sets it to None.
    fn expected_insert_batch<T: serde::Serialize>(
        row: T,
        sentry_received_timestamp: DateTime<Utc>,
        cogs_data: CogsData,
    ) -> InsertBatch {
        InsertBatch {
            rows: RowData::from_rows([row]).unwrap(),
            origin_timestamp: None,
            sentry_received_timestamp: Some(sentry_received_timestamp),
            cogs_data: Some(cogs_data),
            item_type_metrics: None,
        }
    }

    #[test]
    fn test_shouldnt_killswitch() {
        let fake_config = Some("[custom]".to_string());
        let payload = r#"{"use_case_id":"transactions"}"#;

        assert!(!should_use_killswitch(fake_config, payload).unwrap());
    }

    #[test]
    fn test_should_killswitch() {
        let payload = r#"{"use_case_id":"transactions"}"#;
        let fake_config = Some("[transactions]".to_string());

        assert!(should_use_killswitch(fake_config, payload).unwrap());
    }

    #[test]
    fn test_should_killswitch_again() {
        let payload = r#"{"use_case_id":"transactions"}"#;
        let fake_config = Some("[transactions, custom]".to_string());

        assert!(should_use_killswitch(fake_config, payload).unwrap());
    }

    #[test]
    fn test_shouldnt_killswitch_again() {
        let payload = r#"{"use_case_id":"transactions"}"#;
        let fake_config = Some("[]".to_string());

        assert!(!should_use_killswitch(fake_config, payload).unwrap());
    }

    #[test]
    fn test_shouldnt_killswitch_empty() {
        let payload = r#"{"use_case_id":"transactions"}"#;
        let fake_config = Some("".to_string());

        assert!(!should_use_killswitch(fake_config, payload).unwrap());
    }

    #[test]
    fn test_shouldnt_killswitch_no_config() {
        let payload = r#"{"use_case_id":"transactions"}"#;
        let fake_config: Option<String> = None;

        assert!(!should_use_killswitch(fake_config, payload).unwrap());
    }

    #[test]
    fn test_should_record_meta_yes() {
        let use_case_invalid = "escalating_issues";
        assert_eq!(should_record_meta(use_case_invalid), Some(0));

        let use_case_valid = "spans";
        assert_eq!(should_record_meta(use_case_valid), Some(1));

        let use_case_metric_stats = "metric_stats";
        assert_eq!(should_record_meta(use_case_metric_stats), Some(1));
    }

    #[test]
    fn test_validate_timeseries_id() {
        let org_id = 1;
        let project_id = 2;
        let metric_id = 3;
        let mut tags: BTreeMap<&str, String> = BTreeMap::new();
        tags.insert("3", "value3".to_string());
        tags.insert("2", "value2".to_string());
        tags.insert("1", "value1".to_string());

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
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
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
                materialization_version: 3,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            count_value: 1.0,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 615)])
                }
            )
        );
    }

    #[test]
    fn test_counter_processor_with_counter_message_with_sampling_weight() {
        let result = test_processor_with_payload(
            &(process_counter_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_COUNTER_MESSAGE_WITH_SAMPLING_WEIGHT,
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
                materialization_version: 3,
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
                record_meta: Some(1),
                sampling_weight: Some(100),
            },
            count_value: 1.0,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 649)])
                }
            )
        );
    }

    #[test]
    fn test_metric_type_header() {
        assert_eq!(
            MetricTypeHeader::from_kafka_header(None),
            MetricTypeHeader::Unknown
        );
        assert_eq!(
            MetricTypeHeader::from_kafka_header(Some(&Headers::new())),
            MetricTypeHeader::Unknown
        );
        assert_eq!(
            MetricTypeHeader::from_kafka_header(Some(
                &Headers::new().insert("key", Some(b"value".to_vec()))
            )),
            MetricTypeHeader::Unknown
        );

        assert_eq!(
            MetricTypeHeader::from_kafka_header(Some(
                &Headers::new().insert("metric_type", Some(b"c".to_vec()))
            )),
            MetricTypeHeader::Counter
        );
        assert_eq!(
            MetricTypeHeader::from_kafka_header(Some(
                &Headers::new().insert("metric_type", Some(b"s".to_vec()))
            )),
            MetricTypeHeader::Other
        );
    }
}
