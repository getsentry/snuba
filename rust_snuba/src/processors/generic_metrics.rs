use adler::Adler32;
use anyhow::{anyhow, Context, Error};
use chrono::DateTime;
use serde::{
    de::value::{MapAccessDeserializer, SeqAccessDeserializer},
    Deserialize, Deserializer, Serialize,
};
use std::{collections::BTreeMap, marker::PhantomData, vec};

use crate::{
    runtime_config::get_str_config,
    types::{CogsData, InsertBatch, RowData},
    KafkaMessageMetadata, ProcessorConfig,
};

use sentry_arroyo::backends::kafka::types::{Headers, KafkaPayload};
use sentry_arroyo::{counter, timer};
use zstd::stream::decode_all;

use super::utils::enforce_retention;

static BASE64_NOPAD: data_encoding::Encoding = data_encoding::BASE64_NOPAD;

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
    timestamp: f64,
    sentry_received_timestamp: f64,
    tags: BTreeMap<String, String>,
    #[serde(flatten)]
    value: MetricValue,
    retention_days: u16,
    sampling_weight: Option<f64>,
    aggregation_option: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageUseCase {
    use_case_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
enum MetricValue {
    #[serde(rename = "c")]
    Counter(f64),
    #[serde(rename = "s", deserialize_with = "encoded_series_compat_deserializer")]
    Set(EncodedSeries<u32>),
    #[serde(rename = "d", deserialize_with = "encoded_series_compat_deserializer")]
    Distribution(EncodedSeries<f64>),
    #[serde(rename = "g")]
    Gauge {
        count: u64,
        last: f64,
        max: f64,
        min: f64,
        sum: f64,
    },
}

trait Decodable<const SIZE: usize>: Copy {
    const SIZE: usize = SIZE;

    fn decode_bytes(bytes: [u8; SIZE]) -> Self;
}

impl Decodable<4> for u32 {
    fn decode_bytes(bytes: [u8; Self::SIZE]) -> Self {
        Self::from_le_bytes(bytes)
    }
}

impl Decodable<8> for u64 {
    fn decode_bytes(bytes: [u8; Self::SIZE]) -> Self {
        Self::from_le_bytes(bytes)
    }
}

impl Decodable<8> for f64 {
    fn decode_bytes(bytes: [u8; Self::SIZE]) -> Self {
        Self::from_le_bytes(bytes)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format", rename_all = "lowercase")]
enum EncodedSeries<T> {
    Array { data: Vec<T> },
    Base64 { data: String },
    Zstd { data: String },
}

impl<T> EncodedSeries<T> {
    fn try_into_vec<const SIZE: usize>(self) -> Result<Vec<T>, anyhow::Error>
    where
        T: Decodable<SIZE>,
    {
        match self {
            EncodedSeries::Array { data } => Ok(data),
            EncodedSeries::Base64 { data, .. } => {
                counter!("generic_metrics.encoded_receive_count", 1, "format" => "base64");

                let decoded_bytes = BASE64_NOPAD.decode(data.as_bytes())?;
                let res = decode_encoded_into_numeric_array(decoded_bytes)?;

                counter!("generic_metrics.encoded_success_count", 1, "format" => "base64");

                Ok(res)
            }
            EncodedSeries::Zstd { data } => {
                counter!("generic_metrics.encoded_receive_count", 1, "format" => "zstd");

                let decoded_bytes = BASE64_NOPAD.decode(data.as_bytes())?;

                // Convert vec into a slice
                let bytes_slice = decoded_bytes.as_slice();
                let zstd_decompressed = decode_all(bytes_slice)?;
                let res = decode_encoded_into_numeric_array(zstd_decompressed)?;

                counter!("generic_metrics.encoded_success_count", 1, "format" => "zstd");

                Ok(res)
            }
        }
    }
}

fn decode_encoded_into_numeric_array<T, const SIZE: usize>(
    data: Vec<u8>,
) -> Result<Vec<T>, anyhow::Error>
where
    T: Decodable<SIZE>,
{
    if data.len() % T::SIZE == 0 {
        Ok(data
            .chunks_exact(T::SIZE)
            .map(TryInto::try_into)
            .map(Result::unwrap) // OK to unwrap, `chunks_exact` always yields slices of the right length
            .map(T::decode_bytes)
            .collect())
    } else {
        Err(anyhow!(
            "Decoded Base64 cannot be chunked into {}, got {}",
            T::SIZE,
            data.len()
        ))
    }
}

fn encoded_series_compat_deserializer<'de, T, D>(
    deserializer: D,
) -> Result<EncodedSeries<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    struct Visitor<U>(PhantomData<U>);

    impl<'de, U> serde::de::Visitor<'de> for Visitor<U>
    where
        U: Deserialize<'de>,
    {
        type Value = EncodedSeries<U>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("expected either legacy or new distribution value format")
        }

        fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            counter!("generic_metrics.message_count", 1, "format" => "legacy");
            let data = Vec::<U>::deserialize(SeqAccessDeserializer::new(seq))?;
            Ok(EncodedSeries::Array { data })
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            counter!("generic_metrics.message_count", 1, "format" => "encoded");
            EncodedSeries::deserialize(MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(Visitor(PhantomData))
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
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<Self>>;
}

impl Parse for CountersRawRow {
    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<CountersRawRow>> {
        let count_value = match from.value {
            MetricValue::Counter(value) => value,
            _ => return Ok(Option::None),
        };

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
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        let record_meta = should_record_meta(from.use_case_id.as_str());

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "counter".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp as u32,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
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

fn should_use_killswitch(config: Result<Option<String>, Error>, use_case: &MessageUseCase) -> bool {
    if let Some(killswitch) = config.ok().flatten() {
        return killswitch.contains(use_case.use_case_id.as_str());
    }

    false
}

#[allow(dead_code)]
fn parse_matview_ver_from_runtime_opt(metric_type: &str) -> Option<u8> {
    get_str_config(&format!("gen_metrics_{}_matview_version", metric_type))
        .ok()
        .flatten()?
        .parse::<u8>()
        .ok()
}

fn process_message<T>(
    payload: KafkaPayload,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch>
where
    T: Parse + Serialize,
{
    let payload_bytes = payload.payload().context("Expected payload")?;
    let killswitch_config = get_str_config("generic_metrics_use_case_killswitch");
    let use_case: MessageUseCase = serde_json::from_slice(payload_bytes)?;

    if should_use_killswitch(killswitch_config, &use_case) {
        counter!("generic_metrics.messages.killswitched_use_case", 1, "use_case_id" => use_case.use_case_id.as_str());
        return Ok(InsertBatch::skip());
    }

    let msg: FromGenericMetricsMessage = serde_json::from_slice(payload_bytes)?;
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
    Set,
    Distribution,
    Gauge,
}

impl MetricTypeHeader {
    fn from_kafka_header(header: Option<&Headers>) -> Self {
        if let Some(headers) = header {
            if let Some(header_value) = headers.get("metric_type") {
                match header_value {
                    b"c" => MetricTypeHeader::Counter,
                    b"s" => MetricTypeHeader::Set,
                    b"d" => MetricTypeHeader::Distribution,
                    b"g" => MetricTypeHeader::Gauge,
                    _ => MetricTypeHeader::Unknown,
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

/// The raw row that is written to clickhouse for sets.
#[derive(Debug, Serialize, Default)]
struct SetsRawRow {
    #[serde(flatten)]
    common_fields: CommonMetricFields,
    set_values: Vec<u32>,
}

impl Parse for SetsRawRow {
    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<SetsRawRow>> {
        let set_values = match from.value {
            MetricValue::Set(values) => values.try_into_vec()?,
            _ => return Ok(Option::None),
        };

        timer!(
            "generic_metrics.messages.sets_value_len",
            set_values.len() as u64
        );

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();

        timer!("generic_metrics.messages.tags_len", tag_keys.len() as u64, "metric_type" => "set");

        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        if from.aggregation_option.unwrap_or_default() == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        }
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);

        let record_meta = should_record_meta(from.use_case_id.as_str());

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "set".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp as u32,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 2,
            timeseries_id,
            granularities,
            min_retention_days: Some(retention_days as u8),
            record_meta,
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
    let metric_type_header = MetricTypeHeader::from_kafka_header(payload.headers());
    match metric_type_header {
        MetricTypeHeader::Set | MetricTypeHeader::Unknown => {
            process_message::<SetsRawRow>(payload, config)
        }
        _ => Ok(InsertBatch::skip()),
    }
}

#[derive(Debug, Serialize, Default)]
struct DistributionsRawRow {
    #[serde(flatten)]
    common_fields: CommonMetricFields,
    distribution_values: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_histogram: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    disable_percentiles: Option<u8>,
}

impl Parse for DistributionsRawRow {
    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<DistributionsRawRow>> {
        let maybe_dist = match from.value {
            MetricValue::Distribution(value) => value.try_into_vec(),
            _ => return Ok(Option::None),
        };

        let distribution_values = maybe_dist?;

        timer!(
            "generic_metrics.messages.dists_value_len",
            distribution_values.len() as u64
        );

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();

        timer!("generic_metrics.messages.tags_len", tag_keys.len() as u64, "metric_type" => "distribution");

        let mut granularities = vec![
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ];
        let mut enable_histogram = None;
        let mut disable_percentiles = None;
        let aggregate_option = from.aggregation_option.unwrap_or_default();
        if aggregate_option == "ten_second" {
            granularities.push(GRANULARITY_TEN_SECONDS);
        } else if aggregate_option == "hist" {
            enable_histogram = Some(1);
        } else if aggregate_option == "disable_percentiles" {
            disable_percentiles = Some(1);
        }
        let retention_days = enforce_retention(Some(from.retention_days), &config.env_config);
        let record_meta = should_record_meta(from.use_case_id.as_str());

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "distribution".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp as u32,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
            tags_indexed_value: vec![0; tag_keys.len()],
            tags_raw_value: tag_values,
            materialization_version: 4,
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
            distribution_values,
            enable_histogram,
            disable_percentiles,
        }))
    }
}

pub fn process_distribution_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let metric_type_header = MetricTypeHeader::from_kafka_header(payload.headers());
    match metric_type_header {
        MetricTypeHeader::Distribution | MetricTypeHeader::Unknown => {
            process_message::<DistributionsRawRow>(payload, config)
        }
        _ => Ok(InsertBatch::skip()),
    }
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
    fn parse(
        from: FromGenericMetricsMessage,
        config: &ProcessorConfig,
    ) -> anyhow::Result<Option<GaugesRawRow>> {
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
            _ => return Ok(Option::None),
        }

        let timeseries_id =
            generate_timeseries_id(from.org_id, from.project_id, from.metric_id, &from.tags);
        let (tag_keys, tag_values): (Vec<_>, Vec<_>) = from.tags.into_iter().unzip();

        timer!("generic_metrics.messages.tags_len", tag_keys.len() as u64, "metric_type" => "gauge");

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
        let record_meta = should_record_meta(from.use_case_id.as_str());

        let common_fields = CommonMetricFields {
            use_case_id: from.use_case_id,
            org_id: from.org_id,
            project_id: from.project_id,
            metric_type: "gauge".to_string(),
            metric_id: from.metric_id,
            timestamp: from.timestamp as u32,
            retention_days,
            tags_key: tag_keys.iter().map(|k| k.parse::<u64>().unwrap()).collect(),
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
    let metric_type_header = MetricTypeHeader::from_kafka_header(payload.headers());
    match metric_type_header {
        MetricTypeHeader::Gauge | MetricTypeHeader::Unknown => {
            process_message::<GaugesRawRow>(payload, config)
        }
        _ => Ok(InsertBatch::skip()),
    }
}

#[cfg(test)]
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

    const DUMMY_ARR_ENCODED_SET_MESSAGE: &str = r#"{
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
        "value": {"format": "array", "data": [0, 1, 2, 3, 4, 5]}
    }"#;

    const DUMMY_LEGACY_DISTRIBUTION_MESSAGE: &str = r#"{
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

    const DUMMY_ARR_ENCODED_DISTRIBUTION_MESSAGE: &str = r#"{
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
        "value": {"format": "array", "data": [0, 1, 2, 3, 4, 5]}
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

    const DUMMY_DISTRIBUTION_MESSAGE_WITH_PERCENTILE_AGGREGATE_OPTION: &str = r#"{
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
        "aggregation_option": "disable_percentiles"
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

    const DUMMY_GAUGE_MESSAGE_WITH_SAMPLING_WEIGHT: &str = r#"{
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
        "value": {"count": 10, "last": 10.0, "max": 10.0, "min": 1.0, "sum": 20.0},
        "sampling_weight": 100
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

    const DUMMY_BASE64_ENCODED_DISTRIBUTION_MESSAGE: &str = r#"{
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
        "value": {"format": "base64", "data": "AAAAAAAACEAAAAAAAADwPwAAAAAAAABA"}
    }"#;

    const DUMMY_BASE64_ENCODED_SET_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65563,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":"production","9223372036854776017":"healthy","65690":"metric_e2e_spans_dist_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"d":{"65560":"s:spans/duration@second"},"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65691":"metric_e2e_spans_set_k_VUW93LMS"}},
        "type": "s",
        "value": {"format": "base64", "data": "AQAAAAcAAAA"}
    }"#;

    const DUMMY_ZSTD_ENCODED_SET_MESSAGE: &str = r#"{
        "version": 2,
        "use_case_id": "spans",
        "org_id": 1,
        "project_id": 3,
        "metric_id": 65563,
        "timestamp": 1704614940,
        "sentry_received_timestamp": 1704614940,
        "tags": {"9223372036854776010":"production","9223372036854776017":"healthy","65690":"metric_e2e_spans_dist_v_VUW93LMS"},
        "retention_days": 90,
        "mapping_meta":{"d":{"65560":"s:spans/duration@second"},"h":{"9223372036854776017":"session.status","9223372036854776010":"environment"},"f":{"65691":"metric_e2e_spans_set_k_VUW93LMS"}},
        "type": "s",
        "value": {"format": "zstd", "data": "KLUv/QBYQQAAAQAAAAcAAAA"}
    }"#;

    const DUMMY_ZSTD_ENCODED_DISTRIBUTION_MESSAGE: &str = r#"{
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
        "value": {"format": "zstd", "data": "KLUv/QBYrQAAcAAA8D8AQAAAAAAAAAhAAgBgRgCw"}
    }"#;

    const DUMMY_ZSTD_ENCODED_DISTRIBUTION_MESSAGE_WITH_SAMPLING_WEIGHT: &str = r#"{
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
        "value": {"format": "zstd", "data": "KLUv/QBYrQAAcAAA8D8AQAAAAAAAAAhAAgBgRgCw"},
        "sampling_weight": 100
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
    fn test_base64_decode_f64() {
        assert!(
            EncodedSeries::<f64>::Base64 {
                data: "AAAAAAAACEAAAAAAAADwPwAAAAAAAABA".to_string(),
            }
            .try_into_vec()
            .ok()
            .unwrap()
                == vec![3f64, 1f64, 2f64]
        )
    }

    #[test]
    fn test_zstd_decode_f64() {
        assert!(
            EncodedSeries::<f64>::Zstd {
                data: "KLUv/QBYrQAAcAAA8D8AQAAAAAAAAAhAAgBgRgCw".to_string(),
            }
            .try_into_vec()
            .ok()
            .unwrap()
                == vec![1f64, 2f64, 3f64]
        )
    }

    #[test]
    fn test_distribution_processor_with_v2_distribution_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_BASE64_ENCODED_DISTRIBUTION_MESSAGE,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            distribution_values: vec![3f64, 1f64, 2f64],
            enable_histogram: None,
            disable_percentiles: None,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 675)])
                }
            )
        );
    }

    #[test]
    fn test_distribution_processor_with_v3_distribution_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_ZSTD_ENCODED_DISTRIBUTION_MESSAGE,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            distribution_values: vec![1f64, 2f64, 3f64],
            enable_histogram: None,
            disable_percentiles: None,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 681)])
                }
            )
        );
    }

    #[test]
    fn test_distribution_processor_with_weighted_distribution_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_ZSTD_ENCODED_DISTRIBUTION_MESSAGE_WITH_SAMPLING_WEIGHT,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: Some(100),
            },
            distribution_values: vec![1f64, 2f64, 3f64],
            enable_histogram: None,
            disable_percentiles: None,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 713)])
                }
            )
        );
    }

    #[test]
    fn test_base64_decode_32() {
        assert!(
            EncodedSeries::<u32>::Base64 {
                data: "AQAAAAcAAAA".to_string(),
            }
            .try_into_vec()
            .ok()
            .unwrap()
                == vec![1u32, 7u32]
        )
    }

    #[test]
    fn test_zstd_decode_32() {
        assert!(
            EncodedSeries::<u32>::Zstd {
                data: "KLUv/QBYQQAAAQAAAAcAAAA".to_string(),
            }
            .try_into_vec()
            .ok()
            .unwrap()
                == vec![1u32, 7u32]
        )
    }

    #[test]
    fn test_base64_decode_32_invalid() {
        assert!(EncodedSeries::<u32>::Base64 {
            data: "AQAAAAcAAA".to_string(),
        }
        .try_into_vec()
        .is_err())
    }

    #[test]
    fn test_set_processor_with_v2_set_message() {
        let result = test_processor_with_payload(
            &(process_set_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_BASE64_ENCODED_SET_MESSAGE,
        );
        let expected_row = SetsRawRow {
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
                metric_type: "set".to_string(),
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            set_values: vec![1u32, 7u32],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 653)])
                }
            )
        );
    }

    #[test]
    fn test_set_processor_with_v3_set_message() {
        let result = test_processor_with_payload(
            &(process_set_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_ZSTD_ENCODED_SET_MESSAGE,
        );
        let expected_row = SetsRawRow {
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
                metric_type: "set".to_string(),
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            set_values: vec![1u32, 7u32],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 663)])
                }
            )
        );
    }

    #[test]
    fn test_shouldnt_killswitch() {
        let fake_config = Ok(Some("[custom]".to_string()));
        let use_case = MessageUseCase {
            use_case_id: "transactions".to_string(),
        };

        assert!(!should_use_killswitch(fake_config, &use_case));
    }

    #[test]
    fn test_should_killswitch() {
        let use_case = MessageUseCase {
            use_case_id: "transactions".to_string(),
        };
        let fake_config = Ok(Some("[transactions]".to_string()));

        assert!(should_use_killswitch(fake_config, &use_case));
    }

    #[test]
    fn test_should_killswitch_again() {
        let use_case = MessageUseCase {
            use_case_id: "transactions".to_string(),
        };
        let fake_config = Ok(Some("[transactions, custom]".to_string()));

        assert!(should_use_killswitch(fake_config, &use_case));
    }

    #[test]
    fn test_shouldnt_killswitch_again() {
        let use_case = MessageUseCase {
            use_case_id: "transactions".to_string(),
        };
        let fake_config = Ok(Some("[]".to_string()));

        assert!(!should_use_killswitch(fake_config, &use_case));
    }

    #[test]
    fn test_shouldnt_killswitch_empty() {
        let use_case = MessageUseCase {
            use_case_id: "transactions".to_string(),
        };
        let fake_config = Ok(Some("".to_string()));

        assert!(!should_use_killswitch(fake_config, &use_case));
    }

    #[test]
    fn test_shouldnt_killswitch_no_config() {
        let use_case = MessageUseCase {
            use_case_id: "transactions".to_string(),
        };
        let fake_config = Ok(None);

        assert!(!should_use_killswitch(fake_config, &use_case));
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
    fn test_counter_processor_with_set_message() {
        let result = test_processor_with_payload(
            &(process_counter_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_SET_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::skip());
    }

    #[test]
    fn test_set_processor_with_set_message() {
        let result = test_processor_with_payload(
            &(process_set_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            set_values: vec![0, 1, 2, 3, 4, 5],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 622)])
                }
            )
        );
    }

    #[test]
    fn test_set_processor_with_distribution_message() {
        let result = test_processor_with_payload(
            &(process_counter_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_LEGACY_DISTRIBUTION_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::skip());
    }

    #[test]
    fn test_distribution_processor_with_legacy_distribution_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_LEGACY_DISTRIBUTION_MESSAGE,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            enable_histogram: None,
            disable_percentiles: None,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 629)])
                }
            )
        );
    }

    #[test]
    fn test_distribution_processor_with_v1_distribution_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_ARR_ENCODED_DISTRIBUTION_MESSAGE,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            enable_histogram: None,
            disable_percentiles: None,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 658)])
                }
            )
        );
    }

    #[test]
    fn test_distribution_aggregate_option() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            enable_histogram: Some(1),
            disable_percentiles: None,
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 667)])
                }
            )
        );
    }

    #[test]
    fn test_distribution_aggregate_option_percentiles() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_DISTRIBUTION_MESSAGE_WITH_PERCENTILE_AGGREGATE_OPTION,
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
                materialization_version: 4,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            distribution_values: vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            enable_histogram: None,
            disable_percentiles: Some(1),
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 682)])
                }
            )
        );
    }

    #[test]
    fn test_distribution_processor_with_gauge_message() {
        let result = test_processor_with_payload(
            &(process_distribution_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
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
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
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
                materialization_version: 3,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            gauges_values_last: vec![10.0],
            gauges_values_count: vec![10],
            gauges_values_max: vec![10.0],
            gauges_values_min: vec![1.0],
            gauges_values_sum: vec![20.0],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 679)])
                }
            )
        );
    }

    #[test]
    fn test_gauge_processor_with_gauge_message_with_sampling_weight() {
        let result = test_processor_with_payload(
            &(process_gauge_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_GAUGE_MESSAGE_WITH_SAMPLING_WEIGHT,
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
                materialization_version: 3,
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
                record_meta: Some(1),
                sampling_weight: Some(100),
            },
            gauges_values_last: vec![10.0],
            gauges_values_count: vec![10],
            gauges_values_max: vec![10.0],
            gauges_values_min: vec![1.0],
            gauges_values_sum: vec![20.0],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 711)])
                }
            )
        );
    }

    #[test]
    fn test_gauge_processor_with_aggregate_option() {
        let result = test_processor_with_payload(
            &(process_gauge_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
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
                materialization_version: 3,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            gauges_values_last: vec![10.0],
            gauges_values_count: vec![10],
            gauges_values_max: vec![10.0],
            gauges_values_min: vec![1.0],
            gauges_values_sum: vec![20.0],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 719)])
                }
            )
        );
    }

    #[test]
    fn test_gauge_processor_with_counter_message() {
        let result = test_processor_with_payload(
            &(process_gauge_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_COUNTER_MESSAGE,
        );
        assert_eq!(result.unwrap(), InsertBatch::skip());
    }

    #[test]
    fn test_set_processor_with_v1_set_message() {
        let result = test_processor_with_payload(
            &(process_set_message
                as fn(
                    sentry_arroyo::backends::kafka::types::KafkaPayload,
                    crate::types::KafkaMessageMetadata,
                    &crate::ProcessorConfig,
                )
                    -> std::result::Result<crate::types::InsertBatch, anyhow::Error>),
            DUMMY_ARR_ENCODED_SET_MESSAGE,
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
                record_meta: Some(1),
                sampling_weight: None,
            },
            set_values: vec![0, 1, 2, 3, 4, 5],
        };
        assert_eq!(
            result.unwrap(),
            expected_insert_batch(
                expected_row,
                DateTime::from_timestamp(1704614940, 0).unwrap(),
                CogsData {
                    data: BTreeMap::from([("genericmetrics_spans".to_string(), 651)])
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
            MetricTypeHeader::Set
        );
        assert_eq!(
            MetricTypeHeader::from_kafka_header(Some(
                &Headers::new().insert("metric_type", Some(b"d".to_vec()))
            )),
            MetricTypeHeader::Distribution
        );
        assert_eq!(
            MetricTypeHeader::from_kafka_header(Some(
                &Headers::new().insert("metric_type", Some(b"g".to_vec()))
            )),
            MetricTypeHeader::Gauge
        );
    }
}
