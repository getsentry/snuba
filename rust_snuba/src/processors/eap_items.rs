use anyhow::Context;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Utc;
use prost::Message;
use seq_macro::seq;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_protos::snuba::v1::any_value::Value;
use sentry_protos::snuba::v1::{ArrayValue, TraceItem, TraceItemType};

use crate::config::ProcessorConfig;
use crate::processors::utils::{
    enforce_retention, get_drop_invalid_timestamps_enabled, out_of_valid_interval_secs,
    record_invalid_timestamp_metric, SilencedDLQMessage,
};
use crate::runtime_config::get_str_config;
use crate::strategies::clickhouse::rowbinary;
use crate::types::CogsData;
use crate::types::{item_type_name, InsertBatch, ItemTypeMetrics, KafkaMessageMetadata};

/// Runtime config key prefix. Per-storage key
/// `eap_items_dlq_grace_period_min:<storage_name>`: a non-negative integer
/// in minutes. When set, the eap-items processor DLQs any message whose
/// event `timestamp` belongs to a prior weekly partition, but only once
/// we are at least `grace_min` minutes past the most recent Monday 00:00
/// UTC partition boundary. Unset disables the killswitch for that
/// storage (this is how `eap_items_dlq_replay` opts out).
///
/// The eap_items table is partitioned by
/// `(retention_days, toMonday(timestamp))`. Writes that straddle the
/// weekly partition boundary tax ClickHouse; dropping prior-partition
/// messages after a grace window prevents that pressure without
/// over-eagerly dropping current-week stragglers.
const DLQ_GRACE_PERIOD_MIN_KEY: &str = "eap_items_dlq_grace_period_min";

/// Precision factor for sampling_factor calculations to compensate for floating point errors
const SAMPLING_FACTOR_PRECISION: f64 = 1e6;
/// Minimum allowed sampling_factor value
const MIN_SAMPLING_FACTOR: f64 = 1.0 / SAMPLING_FACTOR_PRECISION;

struct ProcessedItem {
    eap_item: EAPItem,
    origin_timestamp: Option<DateTime<Utc>>,
    item_type_metrics: ItemTypeMetrics,
    cogs_data: CogsData,
    should_skip: bool,
}

fn process_eap_item(msg: KafkaPayload, config: &ProcessorConfig) -> anyhow::Result<ProcessedItem> {
    let payload: &[u8] = msg.payload().context("Expected payload")?;
    let trace_item = TraceItem::decode(payload)?;
    let origin_timestamp = trace_item
        .received
        .as_ref()
        .and_then(|received| DateTime::from_timestamp(received.seconds, 0));
    let event_timestamp = trace_item
        .timestamp
        .as_ref()
        .and_then(|ts| DateTime::from_timestamp(ts.seconds, 0));

    let item_type =
        TraceItemType::try_from(trace_item.item_type).unwrap_or(TraceItemType::Unspecified);

    let mut should_skip = false;
    if let Some(event_ts) = event_timestamp {
        let now = Utc::now();

        // should_skip=true will drop messages that are too old or too far in the future
        if get_drop_invalid_timestamps_enabled() && out_of_valid_interval_secs(event_ts, now) {
            let is_future = event_ts > now;
            record_invalid_timestamp_metric("eap_items.messages", is_future, item_type);
            should_skip = true;
        }
        // only DLQ messages that we don't want to drop (when should_skip=false)
        if !should_skip {
            if let Some(grace_min) = get_dlq_grace_period_min(&config.storage_name) {
                if should_dlq_for_prior_partition(event_ts, now, grace_min) {
                    let item_type_str = item_type_name(item_type);
                    counter!("eap_items.messages.dlqed_prior_partition", 1, "item_type" => item_type_str);
                    anyhow::bail!(SilencedDLQMessage);
                }
            }
        }
    }

    let retention_days = Some(enforce_retention(
        Some(trace_item.retention_days as u16),
        &config.env_config,
    ));
    let downsampled_retention_days = if trace_item.downsampled_retention_days > 0 {
        Some(trace_item.downsampled_retention_days as u16)
    } else {
        retention_days
    };

    let mut eap_item = EAPItem::try_from(trace_item)?;

    eap_item.retention_days = retention_days;
    eap_item.downsampled_retention_days = downsampled_retention_days;
    eap_item.attributes.insert_int(
        "sentry._internal.ingested_at".into(),
        Utc::now().timestamp_millis(),
    );
    // we are using this to compare to outcomes which stores the timestamp as seconds
    if let Some(received_at) = origin_timestamp {
        eap_item.attributes.insert_int(
            "sentry._internal.received_at".into(),
            received_at.timestamp(),
        );
    }

    let mut item_type_metrics = ItemTypeMetrics::new();
    item_type_metrics.record_item(item_type, payload.len());

    // COGS tracking by item type
    let app_feature = match item_type {
        TraceItemType::Span => "spans",
        TraceItemType::Error => "errors",
        TraceItemType::Log => "our_logs",
        TraceItemType::UptimeCheck => "uptime",
        TraceItemType::UptimeResult => "uptime",
        TraceItemType::Replay => "replays",
        TraceItemType::Occurrence => "issueplatform",
        TraceItemType::Metric => "trace_metrics",
        TraceItemType::ProfileFunction => "profiles",
        TraceItemType::Attachment => "attachments",
        TraceItemType::Preprod => "preprod",
        TraceItemType::UserSession => "sessions",
        TraceItemType::ProcessingError => "processing_errors",
        TraceItemType::Unspecified => "null",
    }
    .to_string();

    let cogs_data = CogsData {
        data: BTreeMap::from([(app_feature, payload.len() as u64)]),
    };

    Ok(ProcessedItem {
        eap_item,
        origin_timestamp,
        item_type_metrics,
        cogs_data,
        should_skip,
    })
}

fn get_dlq_grace_period_min(storage_name: &str) -> Option<i64> {
    if storage_name.is_empty() {
        return None;
    }
    get_str_config(&format!("{DLQ_GRACE_PERIOD_MIN_KEY}:{storage_name}"))
        .ok()
        .flatten()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n >= 0)
}

/// Most recent Monday 00:00 UTC at or before `now` — the active weekly
/// partition boundary used by `toMonday(timestamp)` in ClickHouse.
fn prior_partition_boundary(now: DateTime<Utc>) -> DateTime<Utc> {
    let days_from_monday = now.weekday().num_days_from_monday() as i64;
    let monday = now.date_naive() - chrono::Duration::days(days_from_monday);
    monday.and_time(chrono::NaiveTime::MIN).and_utc()
}

/// True when we should DLQ this message because it belongs to the prior
/// week's partition and we are far enough past the current partition
/// boundary that prior-week stragglers should no longer be admitted.
fn should_dlq_for_prior_partition(
    event_ts: DateTime<Utc>,
    now: DateTime<Utc>,
    grace_min: i64,
) -> bool {
    let boundary = prior_partition_boundary(now);
    let past_min = now.signed_duration_since(boundary).num_minutes();
    past_min >= grace_min && event_ts < boundary
}

pub fn process_message(
    msg: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let processed = process_eap_item(msg, config)?;
    if processed.should_skip {
        return Ok(InsertBatch::skip());
    }
    let mut batch = InsertBatch::from_rows([processed.eap_item], processed.origin_timestamp)?;
    batch.item_type_metrics = Some(processed.item_type_metrics);
    batch.cogs_data = Some(processed.cogs_data);
    Ok(batch)
}

pub fn process_message_row_binary(
    msg: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let processed = process_eap_item(msg, config)?;
    if processed.should_skip {
        return Ok(InsertBatch::skip());
    }
    let row = EAPItemRow::try_from(processed.eap_item)?;

    // Encode the row to RowBinary bytes inline so the wide typed struct (~80
    // Vec<(String, _)> buckets) drops here instead of riding the pipeline to
    // the writer step. The batch downstream sees only a compact Vec<u8>.
    let mut encoded_rows = Vec::new();
    rowbinary::serialize_into(&mut encoded_rows, &row)?;

    let mut batch = InsertBatch::from_encoded_rows(encoded_rows, 1, processed.origin_timestamp);
    batch.cogs_data = Some(processed.cogs_data);
    batch.item_type_metrics = Some(processed.item_type_metrics);
    Ok(batch)
}

/// Test-only: returns the typed `EAPItemRow` (plus the metadata fields the
/// pipeline carries) without the bytes serialization step. Tests that
/// inspect individual columns use this; the production path goes through
/// `process_message_row_binary` and never holds the typed struct beyond
/// `process_eap_item`.
#[cfg(test)]
pub(crate) struct EAPItemRowBatch {
    pub rows: Vec<EAPItemRow>,
    pub cogs_data: Option<CogsData>,
    pub item_type_metrics: Option<ItemTypeMetrics>,
}

#[cfg(test)]
pub(crate) fn process_message_row_binary_typed(
    msg: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<EAPItemRowBatch> {
    let processed = process_eap_item(msg, config)?;
    if processed.should_skip {
        return Ok(EAPItemRowBatch {
            rows: vec![],
            cogs_data: None,
            item_type_metrics: None,
        });
    }
    Ok(EAPItemRowBatch {
        rows: vec![EAPItemRow::try_from(processed.eap_item)?],
        cogs_data: Some(processed.cogs_data),
        item_type_metrics: Some(processed.item_type_metrics),
    })
}

#[derive(Debug, Default, Serialize)]
struct EAPItem {
    organization_id: u64,
    project_id: u64,
    item_type: u8,
    timestamp: u32,
    trace_id: Uuid,
    item_id: u128,

    /// Per-item-type primary name attribute, promoted to a dedicated column.
    /// Sourced from `sentry.op` for spans and `sentry.metric.name` for metrics.
    indexed_name: String,

    #[serde(flatten)]
    attributes: AttributeMap,

    sampling_factor: f64,
    sampling_weight: u64,

    client_sample_rate: f64,
    server_sample_rate: f64,

    retention_days: Option<u16>,
    downsampled_retention_days: Option<u16>,
}

impl TryFrom<TraceItem> for EAPItem {
    type Error = anyhow::Error;

    fn try_from(from: TraceItem) -> Result<Self, Self::Error> {
        let timestamp = from.timestamp.context("Expected a timestamp")?;

        // Promote the per-item-type primary name attribute into a dedicated
        // `indexed_name` column: `sentry.op` for spans, `sentry.metric.name`
        // for metrics. Read it before `from.attributes` is consumed by the loop
        // below; the attribute is also still written to the attribute maps.
        let item_type =
            TraceItemType::try_from(from.item_type).unwrap_or(TraceItemType::Unspecified);
        let indexed_name = match item_type {
            TraceItemType::Span => Some("sentry.op"),
            TraceItemType::Metric => Some("sentry.metric.name"),
            _ => None,
        }
        .and_then(|key| from.attributes.get(key))
        .and_then(|value| match &value.value {
            Some(Value::StringValue(string)) => Some(string.clone()),
            _ => None,
        })
        .unwrap_or_default();

        let mut eap_item = EAPItem {
            organization_id: from.organization_id,
            project_id: from.project_id,
            item_type: from.item_type as u8,
            trace_id: Uuid::parse_str(&from.trace_id)?,
            item_id: read_item_id(from.item_id)?,
            timestamp: timestamp.seconds as u32,
            indexed_name,
            attributes: Default::default(),
            retention_days: Default::default(),
            downsampled_retention_days: Default::default(),
            sampling_factor: 1.0,
            sampling_weight: 1,
            client_sample_rate: 1.0,
            server_sample_rate: 1.0,
        };

        for (key, value) in from.attributes {
            match value.value {
                Some(Value::StringValue(string)) => eap_item.attributes.insert_string(key, string),
                Some(Value::DoubleValue(double)) => eap_item.attributes.insert_float(key, double),
                Some(Value::IntValue(int)) => eap_item.attributes.insert_int(key, int),
                Some(Value::BoolValue(bool)) => eap_item.attributes.insert_bool(key, bool),
                Some(Value::ArrayValue(array)) => eap_item.attributes.insert_array(key, array),
                Some(Value::BytesValue(_)) => (),
                Some(Value::KvlistValue(_)) => (),
                None => (),
            }
        }

        // Sample rates must be in (0, 1]. Values outside that range are treated as
        // missing and fall back to the default of 1.0. This mirrors the normalization
        // Relay does on `sentry.client_sample_rate` / `sentry.server_sample_rate`.
        if is_valid_sample_rate(from.client_sample_rate) {
            eap_item.sampling_factor *= from.client_sample_rate;
            eap_item.client_sample_rate = from.client_sample_rate;
        }

        if is_valid_sample_rate(from.server_sample_rate) {
            eap_item.sampling_factor *= from.server_sample_rate;
            eap_item.server_sample_rate = from.server_sample_rate;
        }

        // Lower precision to compensate floating point errors.
        eap_item.sampling_factor = (eap_item.sampling_factor * SAMPLING_FACTOR_PRECISION).round()
            / SAMPLING_FACTOR_PRECISION;

        // Ensure sampling_factor has a minimum value to prevent zero
        if eap_item.sampling_factor < MIN_SAMPLING_FACTOR {
            eap_item.sampling_factor = MIN_SAMPLING_FACTOR;
        }

        // Calculate sampling_weight after applying minimum to ensure correct value
        eap_item.sampling_weight = (1.0 / eap_item.sampling_factor).round() as u64;

        Ok(eap_item)
    }
}

fn is_valid_sample_rate(rate: f64) -> bool {
    rate > 0.0 && rate <= 1.0
}

fn fnv_1a(input: &[u8]) -> u32 {
    const FNV_1A_PRIME: u32 = 16777619;
    const FNV_1A_OFFSET_BASIS: u32 = 2166136261;

    let mut res = FNV_1A_OFFSET_BASIS;
    for byt in input {
        res ^= *byt as u32;
        res = res.wrapping_mul(FNV_1A_PRIME);
    }

    res
}

fn read_item_id(from: Vec<u8>) -> anyhow::Result<u128> {
    let bytes: [u8; 16] = from
        .get(..std::mem::size_of::<u128>())
        .ok_or_else(|| anyhow::anyhow!("item_id too short: {} bytes, expected 16", from.len()))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("item_id bytes has wrong length"))?;
    Ok(u128::from_le_bytes(bytes))
}

/// Append `(key, values)` to a typed `Map(String, Array(T))` column, skipping
/// empty arrays. `remaining` is the number of typed buckets this attribute key
/// still has to be written to; the key is moved into the last one and cloned
/// for any earlier buckets. Homogeneous arrays — the common case — touch a
/// single bucket and move the key with no clone.
fn push_typed_array<T>(
    dest: &mut Vec<(String, Vec<T>)>,
    key: &mut String,
    remaining: &mut usize,
    values: Vec<T>,
) {
    if values.is_empty() {
        return;
    }
    *remaining -= 1;
    let key = if *remaining == 0 {
        std::mem::take(key)
    } else {
        key.clone()
    };
    dest.push((key, values));
}

macro_rules! seq_attrs {
    ($($tt:tt)*) => {
        seq!(N in 0..40 {
            $($tt)*
        });
    }
}

#[derive(Debug, Serialize, PartialEq)]
enum EAPValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
}

seq_attrs! {
#[derive(Debug, Default, Serialize)]
struct AttributeMap {
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_bool: HashMap<String, bool>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_int: HashMap<String, i64>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_array: HashMap<String, Vec<EAPValue>>,

    #(
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_string_~N: HashMap<String, String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    attributes_float_~N: HashMap<String, f64>,
    )*
}
}

impl AttributeMap {
    pub fn insert_string(&mut self, k: String, v: String) {
        seq_attrs! {
            let attr_str_buckets = [
                #(
                &mut self.attributes_string_~N,
                )*
            ];
        };

        attr_str_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_str_buckets.len()].insert(k, v);
    }

    pub fn insert_float(&mut self, k: String, v: f64) {
        seq_attrs! {
            let attr_num_buckets = [
                #(
                &mut self.attributes_float_~N,
                )*
            ];
        }

        attr_num_buckets[(fnv_1a(k.as_bytes()) as usize) % attr_num_buckets.len()].insert(k, v);
    }

    pub fn insert_bool(&mut self, k: String, v: bool) {
        self.attributes_bool.insert(k, v);
    }

    pub fn insert_int(&mut self, k: String, v: i64) {
        // double write as float and int
        self.insert_float(k.clone(), v as f64);
        self.attributes_int.insert(k, v);
    }

    pub fn insert_array(&mut self, k: String, v: ArrayValue) {
        let mut values: Vec<EAPValue> = Vec::default();

        for value in v.values {
            match value.value {
                Some(Value::StringValue(string)) => values.push(EAPValue::String(string)),
                Some(Value::DoubleValue(double)) => values.push(EAPValue::Double(double)),
                Some(Value::IntValue(int)) => values.push(EAPValue::Int(int)),
                Some(Value::BoolValue(bool)) => values.push(EAPValue::Bool(bool)),
                Some(Value::BytesValue(_)) => (),
                Some(Value::KvlistValue(_)) => (),
                Some(Value::ArrayValue(_)) => (),
                None => (),
            }
        }

        self.attributes_array.insert(k, values);
    }
}

seq_attrs! {
#[derive(Debug, Clone, Serialize)]
pub struct EAPItemRow {
    organization_id: u64,
    project_id: u64,
    item_type: u8,
    timestamp: u32,
    #[serde(with = "crate::strategies::clickhouse::rowbinary::uuid")]
    trace_id: Uuid,
    item_id: u128,

    indexed_name: String,

    sampling_weight: u64,
    sampling_factor: f64,
    client_sample_rate: f64,
    server_sample_rate: f64,

    retention_days: u16,
    downsampled_retention_days: u16,

    attributes_bool: Vec<(String, bool)>,
    attributes_int: Vec<(String, i64)>,

    #(
    attributes_string_~N: Vec<(String, String)>,
    attributes_float_~N: Vec<(String, f64)>,
    )*

    attributes_array: String,

    attributes_array_string: Vec<(String, Vec<String>)>,
    attributes_array_int: Vec<(String, Vec<i64>)>,
    attributes_array_float: Vec<(String, Vec<f64>)>,
    attributes_array_bool: Vec<(String, Vec<bool>)>,
}
}

seq_attrs! {
impl EAPItemRow {
    /// Column names in struct (= wire) order. We MUST pass this list to
    /// ClickHouse on insert (`INSERT INTO t (col1, col2, ...) FORMAT RowBinary`)
    /// because the on-disk column order in `eap_items_1_local` differs from
    /// the struct order:
    ///
    /// * `client_sample_rate` and `server_sample_rate` were added with
    ///   identical `AFTER sampling_factor` in migration 0048, so the table
    ///   ends up with the pair reversed (server before client).
    /// * The struct interleaves `attributes_string_N, attributes_float_N` for
    ///   each `N` (per the `seq_attrs!` expansion), while the initial table
    ///   put all `attributes_string_*` first, then all `attributes_float_*`.
    ///
    /// Without an explicit column list, ClickHouse falls back to the table's
    /// positional order and misreads bytes (the integration test hits a
    /// `CANNOT_READ_ALL_DATA` deep inside the maps section).
    pub(crate) const COLUMN_NAMES: &'static [&'static str] = &[
        "organization_id",
        "project_id",
        "item_type",
        "timestamp",
        "trace_id",
        "item_id",
        "indexed_name",
        "sampling_weight",
        "sampling_factor",
        "client_sample_rate",
        "server_sample_rate",
        "retention_days",
        "downsampled_retention_days",
        "attributes_bool",
        "attributes_int",
        #(
        concat!("attributes_string_", stringify!(N)),
        concat!("attributes_float_", stringify!(N)),
        )*
        "attributes_array",
        "attributes_array_string",
        "attributes_array_int",
        "attributes_array_float",
        "attributes_array_bool",
    ];
}
}

impl TryFrom<EAPItem> for EAPItemRow {
    type Error = anyhow::Error;

    #[allow(clippy::needless_return)]
    fn try_from(item: EAPItem) -> Result<Self, Self::Error> {
        let attributes_array = serde_json::to_string(&item.attributes.attributes_array)?;

        // Derive the typed `Map(String, Array(T))` columns from the same array
        // attributes, double-writing alongside the `attributes_array` JSON column
        // (serialized just above) so the read path can filter/aggregate on values
        // and enumerate keys via `mapKeys(...)`. We consume the `EAPValue`
        // representation here and move its values into the typed buckets rather
        // than cloning them. Arrays are typically homogeneous, so a key usually
        // lands in a single bucket and is moved, not cloned.
        let mut attributes_array_string: Vec<(String, Vec<String>)> = Vec::new();
        let mut attributes_array_int: Vec<(String, Vec<i64>)> = Vec::new();
        let mut attributes_array_float: Vec<(String, Vec<f64>)> = Vec::new();
        let mut attributes_array_bool: Vec<(String, Vec<bool>)> = Vec::new();
        for (mut key, values) in item.attributes.attributes_array {
            let mut strings: Vec<String> = Vec::new();
            let mut ints: Vec<i64> = Vec::new();
            let mut floats: Vec<f64> = Vec::new();
            let mut bools: Vec<bool> = Vec::new();
            for value in values {
                match value {
                    EAPValue::String(s) => strings.push(s),
                    EAPValue::Int(i) => ints.push(i),
                    EAPValue::Double(d) => floats.push(d),
                    EAPValue::Bool(b) => bools.push(b),
                }
            }
            let mut remaining = usize::from(!strings.is_empty())
                + usize::from(!ints.is_empty())
                + usize::from(!floats.is_empty())
                + usize::from(!bools.is_empty());
            push_typed_array(
                &mut attributes_array_string,
                &mut key,
                &mut remaining,
                strings,
            );
            push_typed_array(&mut attributes_array_int, &mut key, &mut remaining, ints);
            push_typed_array(
                &mut attributes_array_float,
                &mut key,
                &mut remaining,
                floats,
            );
            push_typed_array(&mut attributes_array_bool, &mut key, &mut remaining, bools);
        }

        // `return` is needed because `seq_attrs!` expands with a trailing semicolon,
        // which makes the struct expression a statement rather than a tail expression.
        seq_attrs! {
            return Ok(EAPItemRow {
                organization_id: item.organization_id,
                project_id: item.project_id,
                item_type: item.item_type,
                timestamp: item.timestamp,
                trace_id: item.trace_id,
                item_id: item.item_id,
                indexed_name: item.indexed_name,
                sampling_weight: item.sampling_weight,
                sampling_factor: item.sampling_factor,
                client_sample_rate: item.client_sample_rate,
                server_sample_rate: item.server_sample_rate,
                retention_days: item.retention_days.unwrap_or(0),
                downsampled_retention_days: item.downsampled_retention_days.unwrap_or(0),
                attributes_bool: item.attributes.attributes_bool.into_iter().collect(),
                attributes_int: item.attributes.attributes_int.into_iter().collect(),
                #(
                attributes_string_~N: item.attributes.attributes_string_~N.into_iter().collect(),
                attributes_float_~N: item.attributes.attributes_float_~N.into_iter().collect(),
                )*
                attributes_array,
                attributes_array_string,
                attributes_array_int,
                attributes_array_float,
                attributes_array_bool,
            });
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::SystemTime;

    use prost_types::Timestamp;
    use sentry_protos::snuba::v1::any_value::Value;
    use sentry_protos::snuba::v1::{AnyValue, ArrayValue, TraceItemType};
    use serde::Deserialize;

    use super::*;

    fn generate_trace_item(item_id: Uuid) -> TraceItem {
        TraceItem {
            attributes: Default::default(),
            downsampled_retention_days: Default::default(),
            item_id: item_id.as_u128().to_le_bytes().to_vec(),
            item_type: TraceItemType::Span.into(),
            organization_id: 1,
            project_id: 1,
            received: Some(Timestamp {
                seconds: 1745562493,
                nanos: 0,
            }),
            retention_days: 90,
            timestamp: Some(Timestamp {
                seconds: 1745562493,
                nanos: 0,
            }),
            trace_id: Uuid::new_v4().to_string(),
            client_sample_rate: 1.0,
            server_sample_rate: 1.0,
            outcomes: Default::default(),
        }
    }

    #[test]
    fn test_fnv_1a() {
        assert_eq!(fnv_1a("test".as_bytes()), 2949673445)
    }

    #[test]
    fn test_item_id_is_properly_decoded() {
        let item_id = Uuid::new_v4();
        let trace_item = generate_trace_item(item_id);
        let eap_item = EAPItem::try_from(trace_item);

        assert!(eap_item.is_ok());
        assert_eq!(item_id.as_u128(), eap_item.unwrap().item_id);
    }

    #[test]
    fn test_downsampled_retention_days_default() {
        let item_id = Uuid::new_v4();
        let trace_item = generate_trace_item(item_id);
        let mut payload = Vec::new();

        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let batch = process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        #[derive(Deserialize)]
        pub struct Item {
            retention_days: u16,
            downsampled_retention_days: u16,
        }

        let item: Item = serde_json::from_slice(&batch.rows.encoded_rows).unwrap();

        assert_eq!(item.retention_days, item.downsampled_retention_days);
    }

    #[test]
    fn test_downsampled_retention_days_extended() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);

        trace_item.downsampled_retention_days = 365;

        let mut payload = Vec::new();

        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let batch = process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        #[derive(Deserialize)]
        pub struct Item {
            downsampled_retention_days: u16,
        }

        let item: Item = serde_json::from_slice(&batch.rows.encoded_rows).unwrap();

        assert_eq!(item.downsampled_retention_days, 365);
    }

    #[test]
    fn test_item_type_metrics_accumulated() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);

        // Set the item type to Span (value 1)
        trace_item.item_type = TraceItemType::Span.into();

        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();
        let payload_len = payload_bytes.len();

        let payload = KafkaPayload::new(None, None, Some(payload_bytes));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch = process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        // Verify that item_type_metrics is populated
        assert!(batch.item_type_metrics.is_some());

        let metrics = batch.item_type_metrics.unwrap();

        // Verify that the item_type (Span) has a count of 1
        assert_eq!(metrics.counts.len(), 1);
        assert_eq!(metrics.counts.get(&TraceItemType::Span), Some(&1));

        // Verify that bytes_processed is accumulated
        assert_eq!(metrics.bytes_processed.len(), 1);
        assert_eq!(
            metrics.bytes_processed.get(&TraceItemType::Span),
            Some(&payload_len)
        );
    }

    #[test]
    fn test_item_type_metrics_different_types() {
        // Process first message with item type 1 (Span)
        let item_id_1 = Uuid::new_v4();
        let mut trace_item_1 = generate_trace_item(item_id_1);
        trace_item_1.item_type = TraceItemType::Span.into();

        let mut payload_1 = Vec::new();
        trace_item_1.encode(&mut payload_1).unwrap();

        let payload_1 = KafkaPayload::new(None, None, Some(payload_1));
        let meta_1 = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch_1 = process_message(payload_1, meta_1, &ProcessorConfig::default())
            .expect("The message should be processed");

        // Process second message with item type Log
        let item_id_2 = Uuid::new_v4();
        let mut trace_item_2 = generate_trace_item(item_id_2);
        trace_item_2.item_type = TraceItemType::Log.into();

        let mut payload_2 = Vec::new();
        trace_item_2.encode(&mut payload_2).unwrap();

        let payload_2 = KafkaPayload::new(None, None, Some(payload_2));
        let meta_2 = KafkaMessageMetadata {
            partition: 0,
            offset: 2,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch_2 = process_message(payload_2, meta_2, &ProcessorConfig::default())
            .expect("The message should be processed");

        // Merge the metrics as would happen in the pipeline
        let mut merged_metrics = batch_1.item_type_metrics.unwrap();
        merged_metrics.merge(batch_2.item_type_metrics.unwrap());

        // Verify that both item types are present with count 1 each
        assert_eq!(merged_metrics.counts.len(), 2);
        assert_eq!(merged_metrics.counts.get(&TraceItemType::Span), Some(&1));
        assert_eq!(merged_metrics.counts.get(&TraceItemType::Log), Some(&1));
    }

    #[test]
    fn test_insert_arrays() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);

        trace_item.attributes.insert(
            "arrays".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::IntValue(1234567890)),
                    }],
                })),
            },
        );

        let eap_item = EAPItem::try_from(trace_item);

        assert!(eap_item.is_ok());
        assert_eq!(
            eap_item
                .unwrap()
                .attributes
                .attributes_array
                .get("arrays")
                .unwrap()[0],
            EAPValue::Int(1234567890)
        );
    }

    #[test]
    fn test_received_none() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.received = None;

        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let batch = process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed when received is None");

        assert!(batch.origin_timestamp.is_none());
    }

    #[test]
    fn test_received_at_attribute_is_set() {
        let item_id = Uuid::new_v4();
        let trace_item = generate_trace_item(item_id);
        // generate_trace_item sets received.seconds = 1745562493
        let expected_s: i64 = 1745562493;

        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();

        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        // JSON path
        let json_batch = process_message(
            KafkaPayload::new(None, None, Some(payload_bytes.clone())),
            meta.clone(),
            &ProcessorConfig::default(),
        )
        .expect("The message should be processed");

        #[derive(Deserialize)]
        struct Item {
            #[serde(default)]
            attributes_int: HashMap<String, i64>,
        }
        let item: Item = serde_json::from_slice(&json_batch.rows.encoded_rows).unwrap();
        assert_eq!(
            item.attributes_int.get("sentry._internal.received_at"),
            Some(&expected_s)
        );

        // RowBinary path
        let rb_batch = process_message_row_binary_typed(
            KafkaPayload::new(None, None, Some(payload_bytes)),
            meta,
            &ProcessorConfig::default(),
        )
        .expect("The message should be processed");

        let row = &rb_batch.rows[0];
        let received_at = row
            .attributes_int
            .iter()
            .find(|(k, _)| k == "sentry._internal.received_at")
            .map(|(_, v)| *v);
        assert_eq!(received_at, Some(expected_s));
    }

    #[test]
    fn test_received_at_attribute_absent_when_received_none() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.received = None;

        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();

        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        // JSON path
        let json_batch = process_message(
            KafkaPayload::new(None, None, Some(payload_bytes.clone())),
            meta.clone(),
            &ProcessorConfig::default(),
        )
        .expect("The message should be processed");

        #[derive(Deserialize)]
        struct Item {
            #[serde(default)]
            attributes_int: HashMap<String, i64>,
        }
        let item: Item = serde_json::from_slice(&json_batch.rows.encoded_rows).unwrap();
        assert!(!item
            .attributes_int
            .contains_key("sentry._internal.received_at"));

        // RowBinary path
        let rb_batch = process_message_row_binary_typed(
            KafkaPayload::new(None, None, Some(payload_bytes)),
            meta,
            &ProcessorConfig::default(),
        )
        .expect("The message should be processed");

        let row = &rb_batch.rows[0];
        let has_received_at = row
            .attributes_int
            .iter()
            .any(|(k, _)| k == "sentry._internal.received_at");
        assert!(!has_received_at);
    }

    /// Helper: construct a UTC DateTime from Y-M-D H:M:S.
    fn ymd_hms(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(hh, mm, ss)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn test_prior_partition_boundary_on_monday() {
        // 2026-05-18 is a Monday; boundary is itself at 00:00.
        let now = ymd_hms(2026, 5, 18, 12, 34, 56);
        let boundary = prior_partition_boundary(now);
        assert_eq!(boundary, ymd_hms(2026, 5, 18, 0, 0, 0));
    }

    #[test]
    fn test_prior_partition_boundary_midweek() {
        // 2026-05-21 is a Thursday; boundary is Monday 2026-05-18 00:00.
        let now = ymd_hms(2026, 5, 21, 23, 59, 59);
        let boundary = prior_partition_boundary(now);
        assert_eq!(boundary, ymd_hms(2026, 5, 18, 0, 0, 0));
    }

    #[test]
    fn test_prior_partition_boundary_on_sunday() {
        // 2026-05-24 is a Sunday; boundary is Monday 2026-05-18 00:00.
        let now = ymd_hms(2026, 5, 24, 0, 0, 1);
        let boundary = prior_partition_boundary(now);
        assert_eq!(boundary, ymd_hms(2026, 5, 18, 0, 0, 0));
    }

    #[test]
    fn test_should_dlq_prior_week_past_grace() {
        // 45 min past Monday 00:00 UTC; message timestamp is prior week → DLQ.
        let now = ymd_hms(2026, 5, 18, 0, 45, 0);
        let event_ts = ymd_hms(2026, 5, 17, 23, 30, 0);
        assert!(should_dlq_for_prior_partition(event_ts, now, 45));
    }

    #[test]
    fn test_should_not_dlq_within_grace() {
        // 30 min past Monday 00:00 UTC, grace=45 → not DLQ even though prior week.
        let now = ymd_hms(2026, 5, 18, 0, 30, 0);
        let event_ts = ymd_hms(2026, 5, 17, 23, 30, 0);
        assert!(!should_dlq_for_prior_partition(event_ts, now, 45));
    }

    #[test]
    fn test_should_not_dlq_current_week_past_grace() {
        // Past grace, but message belongs to current week → never DLQ.
        let now = ymd_hms(2026, 5, 18, 1, 30, 0);
        let event_ts = ymd_hms(2026, 5, 18, 1, 0, 0);
        assert!(!should_dlq_for_prior_partition(event_ts, now, 45));
    }

    /// The column list we ship with `INSERT INTO ... FORMAT RowBinary` must
    /// match the struct's field order exactly — otherwise ClickHouse misreads
    /// bytes (e.g., a `Map(String, String)` worth of data lands in a column
    /// declared `Map(String, Float64)`). Lock the order down here so anyone
    /// adding/reordering fields on `EAPItemRow` also updates this list.
    #[test]
    fn test_column_names_match_struct_layout() {
        let names = EAPItemRow::COLUMN_NAMES;
        // 12 scalars + indexed_name + attributes_bool + attributes_int + 80
        // buckets + attributes_array + 4 typed array maps
        assert_eq!(names.len(), 100);
        assert_eq!(names[0], "organization_id");
        assert_eq!(names[5], "item_id");
        assert_eq!(names[6], "indexed_name");
        assert_eq!(names[7], "sampling_weight");
        assert_eq!(names[8], "sampling_factor");
        assert_eq!(names[9], "client_sample_rate");
        assert_eq!(names[10], "server_sample_rate");
        // Bucket pairs are interleaved (string_N then float_N) per the
        // `seq_attrs!` expansion on EAPItemRow.
        assert_eq!(names[15], "attributes_string_0");
        assert_eq!(names[16], "attributes_float_0");
        assert_eq!(names[17], "attributes_string_1");
        assert_eq!(names[93], "attributes_string_39");
        assert_eq!(names[94], "attributes_float_39");
        assert_eq!(names[95], "attributes_array");
        // Typed array map columns follow the JSON column.
        assert_eq!(names[96], "attributes_array_string");
        assert_eq!(names[97], "attributes_array_int");
        assert_eq!(names[98], "attributes_array_float");
        assert_eq!(names[99], "attributes_array_bool");
    }

    #[test]
    fn test_should_not_dlq_event_at_boundary() {
        // event_ts == boundary belongs to the new week — must not DLQ.
        let now = ymd_hms(2026, 5, 18, 6, 0, 0);
        let event_ts = ymd_hms(2026, 5, 18, 0, 0, 0);
        assert!(!should_dlq_for_prior_partition(event_ts, now, 45));
    }

    #[test]
    fn test_should_not_dlq_future_event_ts() {
        // Producer clock skew: event_ts > now. Belongs to current week (or future);
        // event_ts < boundary is false, so we never DLQ.
        let now = ymd_hms(2026, 5, 21, 12, 0, 0);
        let event_ts = ymd_hms(2026, 5, 22, 0, 0, 0);
        assert!(!should_dlq_for_prior_partition(event_ts, now, 45));
    }

    #[test]
    fn test_should_dlq_grace_zero_dlqs_immediately_at_boundary() {
        // grace=0 → DLQ prior-week messages the instant the new week starts.
        let now = ymd_hms(2026, 5, 18, 0, 0, 0);
        let event_ts = ymd_hms(2026, 5, 17, 23, 59, 59);
        assert!(should_dlq_for_prior_partition(event_ts, now, 0));
    }

    #[test]
    fn test_get_dlq_grace_period_min_unset_storage_returns_none() {
        // Empty storage_name short-circuits to None without hitting Python.
        assert_eq!(get_dlq_grace_period_min(""), None);
    }

    #[test]
    fn test_row_binary_basic_processing() {
        let item_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.trace_id = trace_id.to_string();

        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        assert_eq!(batch.rows.len(), 1);
        let row = &batch.rows[0];
        assert_eq!(row.organization_id, 1);
        assert_eq!(row.project_id, 1);
        assert_eq!(row.item_type, TraceItemType::Span as u8);
        assert_eq!(row.timestamp, 1745562493);
        assert_eq!(row.trace_id, trace_id);
        assert_eq!(row.item_id, item_id.as_u128());
        assert_eq!(row.sampling_weight, 1);
        assert!((row.sampling_factor - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_row_binary_downsampled_retention_days_default() {
        let item_id = Uuid::new_v4();
        let trace_item = generate_trace_item(item_id);
        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        let row = &batch.rows[0];
        assert_eq!(row.retention_days, row.downsampled_retention_days);
    }

    #[test]
    fn test_row_binary_downsampled_retention_days_extended() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.downsampled_retention_days = 365;

        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        let row = &batch.rows[0];
        assert_eq!(row.downsampled_retention_days, 365);
    }

    #[test]
    fn test_row_binary_item_type_metrics() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.item_type = TraceItemType::Span.into();

        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();
        let payload_len = payload_bytes.len();

        let payload = KafkaPayload::new(None, None, Some(payload_bytes));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        assert!(batch.item_type_metrics.is_some());
        let metrics = batch.item_type_metrics.unwrap();
        assert_eq!(metrics.counts.get(&TraceItemType::Span), Some(&1));
        assert_eq!(
            metrics.bytes_processed.get(&TraceItemType::Span),
            Some(&payload_len)
        );
    }

    #[test]
    fn test_row_binary_cogs_data() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.item_type = TraceItemType::Log.into();

        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();
        let payload_len = payload_bytes.len();

        let payload = KafkaPayload::new(None, None, Some(payload_bytes));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        assert!(batch.cogs_data.is_some());
        let cogs = batch.cogs_data.unwrap();
        assert_eq!(cogs.data.get("our_logs"), Some(&(payload_len as u64)));
    }

    #[test]
    fn test_row_binary_attributes() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);

        trace_item.attributes.insert(
            "my_int".to_string(),
            AnyValue {
                value: Some(Value::IntValue(42)),
            },
        );
        trace_item.attributes.insert(
            "my_bool".to_string(),
            AnyValue {
                value: Some(Value::BoolValue(true)),
            },
        );
        trace_item.attributes.insert(
            "my_array".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::StringValue("elem".to_string())),
                    }],
                })),
            },
        );

        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        let row = &batch.rows[0];

        // Int attributes are stored in attributes_int (and also double-written to a float bucket)
        assert!(row
            .attributes_int
            .iter()
            .any(|(k, v)| k == "my_int" && *v == 42));

        // Bool attributes are stored in attributes_bool (and also double-written to a float bucket)
        assert!(row
            .attributes_bool
            .iter()
            .any(|(k, v)| k == "my_bool" && *v));

        // Array attributes are serialized as JSON string in the attributes_array column
        assert!(row.attributes_array.contains("my_array"));
        assert!(row.attributes_array.contains("elem"));

        // ...and double-written to the typed `attributes_array_string` column.
        assert!(row
            .attributes_array_string
            .iter()
            .any(|(k, v)| k == "my_array" && v == &vec!["elem".to_string()]));
    }

    #[test]
    fn test_array_typed_columns_double_write() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);

        // Homogeneous arrays of each element type.
        trace_item.attributes.insert(
            "strs".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(Value::StringValue("a".to_string())),
                        },
                        AnyValue {
                            value: Some(Value::StringValue("b".to_string())),
                        },
                    ],
                })),
            },
        );
        trace_item.attributes.insert(
            "ints".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(Value::IntValue(1)),
                        },
                        AnyValue {
                            value: Some(Value::IntValue(2)),
                        },
                    ],
                })),
            },
        );
        trace_item.attributes.insert(
            "floats".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::DoubleValue(1.5)),
                    }],
                })),
            },
        );
        trace_item.attributes.insert(
            "bools".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::BoolValue(true)),
                    }],
                })),
            },
        );

        let row = EAPItemRow::try_from(EAPItem::try_from(trace_item).unwrap()).unwrap();

        fn get<'a, T>(col: &'a [(String, T)], key: &str) -> Option<&'a T> {
            col.iter().find(|(k, _)| k == key).map(|(_, v)| v)
        }

        assert_eq!(
            get(&row.attributes_array_string, "strs"),
            Some(&vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(get(&row.attributes_array_int, "ints"), Some(&vec![1i64, 2]));
        assert_eq!(
            get(&row.attributes_array_float, "floats"),
            Some(&vec![1.5f64])
        );
        assert_eq!(get(&row.attributes_array_bool, "bools"), Some(&vec![true]));

        // Ints are NOT double-written to the float column.
        assert_eq!(get(&row.attributes_array_float, "ints"), None);
        // Typed maps are not cross-populated across element types.
        assert_eq!(get(&row.attributes_array_int, "strs"), None);
        assert_eq!(get(&row.attributes_array_bool, "ints"), None);

        // The legacy JSON column is still populated (double write).
        assert!(row.attributes_array.contains("strs"));
        assert!(row.attributes_array.contains("ints"));
    }

    #[test]
    fn test_indexed_name_from_sentry_op_for_spans() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.item_type = TraceItemType::Span.into();
        trace_item.attributes.insert(
            "sentry.op".to_string(),
            AnyValue {
                value: Some(Value::StringValue("db.query".to_string())),
            },
        );

        let eap_item = EAPItem::try_from(trace_item).unwrap();
        assert_eq!(eap_item.indexed_name, "db.query");
    }

    #[test]
    fn test_indexed_name_from_sentry_metric_name_for_metrics() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.item_type = TraceItemType::Metric.into();
        trace_item.attributes.insert(
            "sentry.metric.name".to_string(),
            AnyValue {
                value: Some(Value::StringValue("my.metric".to_string())),
            },
        );

        let eap_item = EAPItem::try_from(trace_item).unwrap();
        assert_eq!(eap_item.indexed_name, "my.metric");
    }

    #[test]
    fn test_indexed_name_empty_when_source_attribute_missing() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.item_type = TraceItemType::Span.into();
        // No sentry.op attribute set.

        let eap_item = EAPItem::try_from(trace_item).unwrap();
        assert_eq!(eap_item.indexed_name, "");
    }

    #[test]
    fn test_indexed_name_serialized_in_row_binary() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.item_type = TraceItemType::Span.into();
        trace_item.attributes.insert(
            "sentry.op".to_string(),
            AnyValue {
                value: Some(Value::StringValue("http.server".to_string())),
            },
        );

        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        assert_eq!(batch.rows[0].indexed_name, "http.server");
    }

    #[test]
    fn test_row_binary_sampling_rates() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.client_sample_rate = 0.5;
        trace_item.server_sample_rate = 0.25;

        let mut payload = Vec::new();
        trace_item.encode(&mut payload).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        let batch = process_message_row_binary_typed(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        let row = &batch.rows[0];
        assert_eq!(row.sampling_weight, 8); // 1 / (0.5 * 0.25) = 8
        assert!((row.sampling_factor - 0.125).abs() < 1e-6);
    }

    /// Compares the output of process_message (JSON path) and process_message_row_binary
    /// (RowBinary path) to ensure both produce equivalent data for the same input.
    #[test]
    fn test_json_and_row_binary_produce_equivalent_output() {
        let item_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();

        // Build a trace item with various attribute types
        let mut trace_item = generate_trace_item(item_id);
        trace_item.trace_id = trace_id.to_string();
        trace_item.client_sample_rate = 0.5;
        trace_item.server_sample_rate = 0.25;
        trace_item.downsampled_retention_days = 365;
        trace_item.attributes.insert(
            "str_attr".to_string(),
            AnyValue {
                value: Some(Value::StringValue("hello".to_string())),
            },
        );
        trace_item.attributes.insert(
            "int_attr".to_string(),
            AnyValue {
                value: Some(Value::IntValue(42)),
            },
        );
        trace_item.attributes.insert(
            "float_attr".to_string(),
            AnyValue {
                value: Some(Value::DoubleValue(1.5)),
            },
        );
        trace_item.attributes.insert(
            "bool_attr".to_string(),
            AnyValue {
                value: Some(Value::BoolValue(true)),
            },
        );
        trace_item.attributes.insert(
            "array_attr".to_string(),
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(Value::StringValue("a".to_string())),
                        },
                        AnyValue {
                            value: Some(Value::IntValue(1)),
                        },
                    ],
                })),
            },
        );

        // Encode the protobuf once, then clone for both paths
        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();

        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        // Process through JSON path
        let json_payload = KafkaPayload::new(None, None, Some(payload_bytes.clone()));
        let json_batch = process_message(json_payload, meta.clone(), &ProcessorConfig::default())
            .expect("JSON path should succeed");

        // Process through RowBinary path
        let rb_payload = KafkaPayload::new(None, None, Some(payload_bytes));
        let rb_batch =
            process_message_row_binary_typed(rb_payload, meta, &ProcessorConfig::default())
                .expect("RowBinary path should succeed");

        // Parse the JSON output
        let json_str =
            std::str::from_utf8(&json_batch.rows.encoded_rows).expect("JSON should be valid UTF-8");
        let json_row: serde_json::Value =
            serde_json::from_str(json_str.trim()).expect("Should parse as JSON");

        let rb_row = &rb_batch.rows[0];

        // Compare scalar fields
        assert_eq!(
            json_row["organization_id"].as_u64().unwrap(),
            rb_row.organization_id,
            "organization_id mismatch"
        );
        assert_eq!(
            json_row["project_id"].as_u64().unwrap(),
            rb_row.project_id,
            "project_id mismatch"
        );
        assert_eq!(
            json_row["item_type"].as_u64().unwrap() as u8,
            rb_row.item_type,
            "item_type mismatch"
        );
        assert_eq!(
            json_row["timestamp"].as_u64().unwrap() as u32,
            rb_row.timestamp,
            "timestamp mismatch"
        );
        assert_eq!(
            json_row["sampling_weight"].as_u64().unwrap(),
            rb_row.sampling_weight,
            "sampling_weight mismatch"
        );
        assert!(
            (json_row["sampling_factor"].as_f64().unwrap() - rb_row.sampling_factor).abs() < 1e-9,
            "sampling_factor mismatch"
        );
        assert_eq!(
            json_row["retention_days"].as_u64().unwrap() as u16,
            rb_row.retention_days,
            "retention_days mismatch"
        );
        assert_eq!(
            json_row["downsampled_retention_days"].as_u64().unwrap() as u16,
            rb_row.downsampled_retention_days,
            "downsampled_retention_days mismatch"
        );

        // Compare trace_id (JSON serializes as hyphenated UUID string)
        let json_trace_id = Uuid::parse_str(json_row["trace_id"].as_str().unwrap()).unwrap();
        assert_eq!(json_trace_id, rb_row.trace_id, "trace_id mismatch");

        // Compare item_id — u128 may exceed u64 range. serde_json serializes large u128
        // values as floats which lose precision. Both paths go through the same
        // process_eap_item → read_item_id, so we verify via the shared trace_id lookup
        // and check the JSON can at least represent the value.
        // For small item_ids that fit in u64:
        if let Some(id) = json_row["item_id"].as_u64() {
            assert_eq!(id as u128, rb_row.item_id, "item_id mismatch");
        }
        // For all cases, the RowBinary item_id should match what we generated
        assert_eq!(
            rb_row.item_id,
            item_id.as_u128(),
            "item_id mismatch vs input"
        );

        // Compare attributes_int (excluding sentry._internal.ingested_at which uses
        // Utc::now() and may differ by a millisecond between the two calls)
        if let Some(json_ints) = json_row.get("attributes_int") {
            let mut json_ints: HashMap<String, i64> =
                serde_json::from_value(json_ints.clone()).unwrap();
            json_ints.remove("sentry._internal.ingested_at");
            let rb_ints: HashMap<String, i64> = rb_row
                .attributes_int
                .iter()
                .filter(|(k, _)| k != "sentry._internal.ingested_at")
                .cloned()
                .collect();
            assert_eq!(json_ints, rb_ints, "attributes_int mismatch");
        }

        // Compare attributes_bool
        if let Some(json_bools) = json_row.get("attributes_bool") {
            let json_bools: HashMap<String, bool> =
                serde_json::from_value(json_bools.clone()).unwrap();
            let rb_bools: HashMap<String, bool> = rb_row.attributes_bool.iter().cloned().collect();
            assert_eq!(json_bools, rb_bools, "attributes_bool mismatch");
        }

        // Verify bucketed string attributes have the same content
        // "str_attr" should be in one of the string buckets
        let str_bucket = (fnv_1a("str_attr".as_bytes()) as usize) % 40;
        let json_field = format!("attributes_string_{str_bucket}");
        let json_str_map: HashMap<String, String> = json_row
            .get(&json_field)
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .unwrap_or_default();
        assert_eq!(
            json_str_map.get("str_attr"),
            Some(&"hello".to_string()),
            "str_attr not found in JSON output"
        );

        // The same bucket in RowBinary row should match
        // We need to access the field dynamically - use the known bucket index
        // Helper to find a value in a Vec<(String, String)> by key
        fn find_str_in_vec<'a>(vec: &'a [(String, String)], key: &str) -> Option<&'a String> {
            vec.iter().find(|(k, _)| k == key).map(|(_, v)| v)
        }
        let rb_str_val = match str_bucket {
            0 => find_str_in_vec(&rb_row.attributes_string_0, "str_attr"),
            1 => find_str_in_vec(&rb_row.attributes_string_1, "str_attr"),
            2 => find_str_in_vec(&rb_row.attributes_string_2, "str_attr"),
            3 => find_str_in_vec(&rb_row.attributes_string_3, "str_attr"),
            4 => find_str_in_vec(&rb_row.attributes_string_4, "str_attr"),
            5 => find_str_in_vec(&rb_row.attributes_string_5, "str_attr"),
            6 => find_str_in_vec(&rb_row.attributes_string_6, "str_attr"),
            7 => find_str_in_vec(&rb_row.attributes_string_7, "str_attr"),
            8 => find_str_in_vec(&rb_row.attributes_string_8, "str_attr"),
            9 => find_str_in_vec(&rb_row.attributes_string_9, "str_attr"),
            10 => find_str_in_vec(&rb_row.attributes_string_10, "str_attr"),
            11 => find_str_in_vec(&rb_row.attributes_string_11, "str_attr"),
            12 => find_str_in_vec(&rb_row.attributes_string_12, "str_attr"),
            13 => find_str_in_vec(&rb_row.attributes_string_13, "str_attr"),
            14 => find_str_in_vec(&rb_row.attributes_string_14, "str_attr"),
            15 => find_str_in_vec(&rb_row.attributes_string_15, "str_attr"),
            16 => find_str_in_vec(&rb_row.attributes_string_16, "str_attr"),
            17 => find_str_in_vec(&rb_row.attributes_string_17, "str_attr"),
            18 => find_str_in_vec(&rb_row.attributes_string_18, "str_attr"),
            19 => find_str_in_vec(&rb_row.attributes_string_19, "str_attr"),
            20 => find_str_in_vec(&rb_row.attributes_string_20, "str_attr"),
            21 => find_str_in_vec(&rb_row.attributes_string_21, "str_attr"),
            22 => find_str_in_vec(&rb_row.attributes_string_22, "str_attr"),
            23 => find_str_in_vec(&rb_row.attributes_string_23, "str_attr"),
            24 => find_str_in_vec(&rb_row.attributes_string_24, "str_attr"),
            25 => find_str_in_vec(&rb_row.attributes_string_25, "str_attr"),
            26 => find_str_in_vec(&rb_row.attributes_string_26, "str_attr"),
            27 => find_str_in_vec(&rb_row.attributes_string_27, "str_attr"),
            28 => find_str_in_vec(&rb_row.attributes_string_28, "str_attr"),
            29 => find_str_in_vec(&rb_row.attributes_string_29, "str_attr"),
            30 => find_str_in_vec(&rb_row.attributes_string_30, "str_attr"),
            31 => find_str_in_vec(&rb_row.attributes_string_31, "str_attr"),
            32 => find_str_in_vec(&rb_row.attributes_string_32, "str_attr"),
            33 => find_str_in_vec(&rb_row.attributes_string_33, "str_attr"),
            34 => find_str_in_vec(&rb_row.attributes_string_34, "str_attr"),
            35 => find_str_in_vec(&rb_row.attributes_string_35, "str_attr"),
            36 => find_str_in_vec(&rb_row.attributes_string_36, "str_attr"),
            37 => find_str_in_vec(&rb_row.attributes_string_37, "str_attr"),
            38 => find_str_in_vec(&rb_row.attributes_string_38, "str_attr"),
            39 => find_str_in_vec(&rb_row.attributes_string_39, "str_attr"),
            _ => unreachable!(),
        };
        assert_eq!(
            rb_str_val,
            Some(&"hello".to_string()),
            "str_attr not found in RowBinary output bucket {str_bucket}"
        );
        assert_eq!(
            json_str_map.get("str_attr"),
            rb_str_val,
            "str_attr differs between JSON and RowBinary"
        );

        // Verify float attribute is in the correct bucket (int_attr and bool_attr are
        // double-written as floats)
        let float_bucket = (fnv_1a("float_attr".as_bytes()) as usize) % 40;
        let json_float_field = format!("attributes_float_{float_bucket}");
        let json_float_map: HashMap<String, f64> = json_row
            .get(&json_float_field)
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .unwrap_or_default();
        assert!(
            (json_float_map.get("float_attr").copied().unwrap_or(0.0) - 1.5).abs() < 1e-9,
            "float_attr mismatch in JSON"
        );

        // Compare attributes_array
        // In JSON: serialized as {"array_attr": [{"String": "a"}, {"Int": 1}]}
        // In RowBinary: serialized as a JSON string in the attributes_array field
        if let Some(json_arrays) = json_row.get("attributes_array") {
            let rb_arrays: serde_json::Value =
                serde_json::from_str(&rb_row.attributes_array).unwrap_or_default();
            assert_eq!(
                json_arrays, &rb_arrays,
                "attributes_array mismatch between JSON and RowBinary"
            );
        }

        // Compare cogs_data
        assert_eq!(
            json_batch.cogs_data, rb_batch.cogs_data,
            "cogs_data mismatch"
        );

        // Compare item_type_metrics
        assert_eq!(
            json_batch.item_type_metrics, rb_batch.item_type_metrics,
            "item_type_metrics mismatch"
        );
    }

    /// End-to-end test of the production RowBinary path against a live
    /// ClickHouse instance: process_message_row_binary produces bytes via the
    /// vendored serializer, those bytes are POSTed verbatim with
    /// `FORMAT RowBinary`, and the row is read back via `FORMAT JSON`. This
    /// is the cross-boundary check that our wire format matches what
    /// ClickHouse expects — pure unit tests can't catch that.
    #[tokio::test]
    async fn test_row_binary_clickhouse_insert() {
        let host = std::env::var("CLICKHOUSE_HOST").unwrap_or("127.0.0.1".to_string());
        let http_port: u16 = std::env::var("CLICKHOUSE_HTTP_PORT")
            .unwrap_or("8123".to_string())
            .parse()
            .unwrap();
        let database = std::env::var("CLICKHOUSE_DATABASE").unwrap_or("default".to_string());
        let base_url = format!("http://{host}:{http_port}");

        let http = reqwest::Client::new();

        // Use a unique organization_id to avoid conflicts with other test data
        let unique_org_id: u64 = 999_999_000 + (rand::random::<u32>() % 1000) as u64;

        // Use current timestamp so TTL (timestamp + retention_days) is in the future
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let item_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.trace_id = trace_id.to_string();
        trace_item.organization_id = unique_org_id;
        trace_item.timestamp = Some(prost_types::Timestamp {
            seconds: now_secs,
            nanos: 0,
        });
        trace_item.received = Some(prost_types::Timestamp {
            seconds: now_secs,
            nanos: 0,
        });
        trace_item.attributes.insert(
            "test_string".to_string(),
            sentry_protos::snuba::v1::AnyValue {
                value: Some(Value::StringValue("hello".to_string())),
            },
        );
        trace_item.attributes.insert(
            "test_int".to_string(),
            sentry_protos::snuba::v1::AnyValue {
                value: Some(Value::IntValue(42)),
            },
        );

        let mut payload_bytes = Vec::new();
        trace_item.encode(&mut payload_bytes).unwrap();

        let payload = KafkaPayload::new(None, None, Some(payload_bytes));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };

        // Production path: encodes the row to RowBinary bytes inside the processor.
        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
        assert_eq!(batch.rows.num_rows, 1);

        // Insert: POST the pre-encoded bytes with FORMAT RowBinary. We must
        // pass the column list — the struct's wire order does NOT match the
        // table's on-disk column order (see EAPItemRow::COLUMN_NAMES).
        let insert_query = format!(
            "INSERT INTO eap_items_1_local ({}) FORMAT RowBinary",
            EAPItemRow::COLUMN_NAMES.join(", "),
        );
        let insert_resp = http
            .post(&base_url)
            .header("X-ClickHouse-Database", &database)
            .query(&[
                ("query", insert_query.as_str()),
                ("input_format_binary_read_json_as_string", "1"),
                ("insert_deduplicate", "0"),
            ])
            .body(batch.rows.encoded_rows.clone())
            .send()
            .await
            .expect("Insert request failed to send");
        assert!(
            insert_resp.status().is_success(),
            "Insert failed: {}",
            insert_resp.text().await.unwrap_or_default()
        );

        // Read it back via FORMAT JSON. We use organization_id (primary key prefix)
        // for a deterministic lookup. ClickHouse's FORMAT JSON renders 64-bit
        // ints as strings to avoid JS-precision loss, so we parse them back.
        //
        // GET (not POST) for the read-back: ClickHouse 25.x rejects bodyless
        // POSTs with HTTP 411 because reqwest doesn't emit a Content-Length
        // header by default when there's nothing to send.
        let select_resp = http
            .get(&base_url)
            .header("X-ClickHouse-Database", &database)
            .query(&[(
                "query",
                format!(
                    "SELECT organization_id, project_id, item_type, sampling_weight \
                     FROM eap_items_1_local \
                     WHERE organization_id = {unique_org_id} \
                     LIMIT 1 FORMAT JSON"
                ),
            )])
            .send()
            .await
            .expect("Select request failed to send");
        let select_status = select_resp.status();
        let body_text = select_resp.text().await.expect("response body");
        assert!(
            select_status.is_success(),
            "Select failed: status={select_status}, body={body_text}"
        );
        let body: serde_json::Value = serde_json::from_str(&body_text).expect("JSON response");
        let data = body["data"].as_array().expect("data array");
        assert_eq!(data.len(), 1, "no rows found for org_id={unique_org_id}");
        let row = &data[0];
        assert_eq!(
            row["organization_id"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            unique_org_id
        );
        assert_eq!(
            row["project_id"].as_str().unwrap().parse::<u64>().unwrap(),
            1
        );
        assert_eq!(
            row["item_type"].as_u64().unwrap() as u8,
            TraceItemType::Span as u8
        );
        assert_eq!(
            row["sampling_weight"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            1
        );

        // Clean up. POST with an empty body (rather than no body) so reqwest
        // emits Content-Length: 0 and ClickHouse 25.x doesn't reject with 411.
        let _ = http
            .post(&base_url)
            .header("X-ClickHouse-Database", &database)
            .query(&[(
                "query",
                format!(
                    "ALTER TABLE eap_items_1_local DELETE WHERE organization_id = {unique_org_id}"
                ),
            )])
            .body("")
            .send()
            .await;
    }

    #[test]
    fn test_out_of_range_sample_rates_are_normalized_to_one() {
        for (client, server) in [
            (1.5, 0.5),  // client > 1
            (0.5, 1.5),  // server > 1
            (-0.1, 0.5), // client < 0
            (0.5, -0.1), // server < 0
            (0.0, 0.5),  // client == 0
            (0.5, 0.0),  // server == 0
        ] {
            let item_id = Uuid::new_v4();
            let mut trace_item = generate_trace_item(item_id);
            trace_item.client_sample_rate = client;
            trace_item.server_sample_rate = server;

            let eap_item = EAPItem::try_from(trace_item).expect("conversion should succeed");

            let expected_client = if is_valid_sample_rate(client) {
                client
            } else {
                1.0
            };
            let expected_server = if is_valid_sample_rate(server) {
                server
            } else {
                1.0
            };
            let expected_factor = expected_client * expected_server;

            assert_eq!(
                eap_item.client_sample_rate, expected_client,
                "client_sample_rate {client} should normalize to {expected_client}"
            );
            assert_eq!(
                eap_item.server_sample_rate, expected_server,
                "server_sample_rate {server} should normalize to {expected_server}"
            );
            assert!(
                (eap_item.sampling_factor - expected_factor).abs() < 1e-9,
                "sampling_factor for ({client}, {server}) should be {expected_factor}, got {}",
                eap_item.sampling_factor
            );
        }
    }

    #[test]
    fn test_nan_sample_rates_are_normalized_to_one() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);
        trace_item.client_sample_rate = f64::NAN;
        trace_item.server_sample_rate = f64::NAN;

        let eap_item = EAPItem::try_from(trace_item).expect("conversion should succeed");

        assert_eq!(eap_item.client_sample_rate, 1.0);
        assert_eq!(eap_item.server_sample_rate, 1.0);
        assert_eq!(eap_item.sampling_factor, 1.0);
        assert_eq!(eap_item.sampling_weight, 1);
    }

    #[test]
    fn test_very_low_sample_rates_do_not_result_in_zero_sampling_factor() {
        let item_id = Uuid::new_v4();
        let mut trace_item = generate_trace_item(item_id);

        // Set extremely low sample rates that would multiply to a value smaller than 1e-6
        trace_item.client_sample_rate = 0.001; // 1e-3
        trace_item.server_sample_rate = 0.0001; // 1e-4
                                                // Combined: 1e-7, which is smaller than MIN_SAMPLING_FACTOR (1e-6)

        let eap_item = EAPItem::try_from(trace_item);

        assert!(eap_item.is_ok());
        let eap_item = eap_item.unwrap();

        // Verify that sampling_factor is not zero and equals the minimum
        assert_eq!(eap_item.sampling_factor, MIN_SAMPLING_FACTOR);
        assert!(eap_item.sampling_factor > 0.0);

        // Verify that sampling_weight is calculated correctly
        assert_eq!(
            eap_item.sampling_weight,
            (1.0 / MIN_SAMPLING_FACTOR).round() as u64
        );
    }
}
