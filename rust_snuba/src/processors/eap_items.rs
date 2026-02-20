use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use seq_macro::seq;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_protos::snuba::v1::any_value::Value;
use sentry_protos::snuba::v1::{ArrayValue, TraceItem, TraceItemType};

use crate::config::ProcessorConfig;
use crate::processors::utils::enforce_retention;
use crate::types::CogsData;
use crate::types::{InsertBatch, ItemTypeMetrics, KafkaMessageMetadata, TypedInsertBatch};

struct ProcessedItem {
    eap_item: EAPItem,
    origin_timestamp: Option<DateTime<Utc>>,
    item_type_metrics: ItemTypeMetrics,
    cogs_data: CogsData,
}

fn process_eap_item(msg: KafkaPayload, config: &ProcessorConfig) -> anyhow::Result<ProcessedItem> {
    let payload: &[u8] = msg.payload().context("Expected payload")?;
    let trace_item = TraceItem::decode(payload)?;
    let origin_timestamp = DateTime::from_timestamp(trace_item.received.unwrap().seconds, 0);
    let retention_days = Some(enforce_retention(
        Some(trace_item.retention_days as u16),
        &config.env_config,
    ));
    let downsampled_retention_days = if trace_item.downsampled_retention_days > 0 {
        Some(trace_item.downsampled_retention_days as u16)
    } else {
        retention_days
    };

    let item_type =
        TraceItemType::try_from(trace_item.item_type).unwrap_or(TraceItemType::Unspecified);
    let mut eap_item = EAPItem::try_from(trace_item)?;

    eap_item.retention_days = retention_days;
    eap_item.downsampled_retention_days = downsampled_retention_days;
    eap_item.attributes.insert_int(
        "sentry._internal.ingested_at".into(),
        Utc::now().timestamp_millis(),
    );

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
        TraceItemType::Metric => "sessions",
        TraceItemType::ProfileFunction => "profiles",
        TraceItemType::Attachment => "attachments",
        TraceItemType::Preprod => "preprod",
        TraceItemType::UserSession => "sessions",
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
    })
}

pub fn process_message(
    msg: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let processed = process_eap_item(msg, config)?;
    let mut batch = InsertBatch::from_rows([processed.eap_item], processed.origin_timestamp)?;
    batch.item_type_metrics = Some(processed.item_type_metrics);
    batch.cogs_data = Some(processed.cogs_data);
    Ok(batch)
}

pub fn process_message_row_binary(
    msg: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<TypedInsertBatch<EAPItemRow>> {
    let processed = process_eap_item(msg, config)?;
    let mut batch = TypedInsertBatch::from_rows(
        vec![EAPItemRow::from(processed.eap_item)],
        processed.origin_timestamp,
    );
    batch.item_type_metrics = Some(processed.item_type_metrics);
    batch.cogs_data = Some(processed.cogs_data);
    Ok(batch)
}

#[derive(Debug, Default, Serialize)]
struct EAPItem {
    organization_id: u64,
    project_id: u64,
    item_type: u8,
    timestamp: u32,
    trace_id: Uuid,
    item_id: u128,

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
        let mut eap_item = EAPItem {
            organization_id: from.organization_id,
            project_id: from.project_id,
            item_type: from.item_type as u8,
            trace_id: Uuid::parse_str(&from.trace_id)?,
            item_id: read_item_id(from.item_id),
            timestamp: timestamp.seconds as u32,
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

        if from.client_sample_rate > 0.0 {
            eap_item.sampling_factor *= from.client_sample_rate;
            eap_item.client_sample_rate = from.client_sample_rate;
        }

        if from.server_sample_rate > 0.0 {
            eap_item.sampling_factor *= from.server_sample_rate;
            eap_item.server_sample_rate = from.server_sample_rate;
        }

        // Lower precision to compensate floating point errors.
        eap_item.sampling_weight = (1.0 / eap_item.sampling_factor).round() as u64;
        eap_item.sampling_factor = (eap_item.sampling_factor * 1e9).round() / 1e9;

        Ok(eap_item)
    }
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

fn read_item_id(from: Vec<u8>) -> u128 {
    let (item_id_bytes, _) = from.split_at(std::mem::size_of::<u128>());
    u128::from_le_bytes(item_id_bytes.try_into().unwrap())
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
        // double write as float and bool
        self.insert_float(k.clone(), v as u8 as f64);
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
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct EAPItemRow {
    organization_id: u64,
    project_id: u64,
    item_type: u8,
    timestamp: u32,
    #[serde(with = "clickhouse::serde::uuid")]
    trace_id: Uuid,
    item_id: u128,

    sampling_weight: u64,
    sampling_factor: f64,

    retention_days: u16,
    downsampled_retention_days: u16,

    attributes_bool: Vec<(String, bool)>,
    attributes_int: Vec<(String, i64)>,

    #(
    attributes_string_~N: Vec<(String, String)>,
    attributes_float_~N: Vec<(String, f64)>,
    )*

    attributes_array: String,
}
}

impl From<EAPItem> for EAPItemRow {
    #[allow(clippy::needless_return)]
    fn from(item: EAPItem) -> Self {
        // `return` is needed because `seq_attrs!` expands with a trailing semicolon,
        // which makes the struct expression a statement rather than a tail expression.
        seq_attrs! {
            return EAPItemRow {
                organization_id: item.organization_id,
                project_id: item.project_id,
                item_type: item.item_type,
                timestamp: item.timestamp,
                trace_id: item.trace_id,
                item_id: item.item_id,
                sampling_weight: item.sampling_weight,
                sampling_factor: item.sampling_factor,
                retention_days: item.retention_days.unwrap_or(0),
                downsampled_retention_days: item.downsampled_retention_days.unwrap_or(0),
                attributes_bool: item.attributes.attributes_bool.into_iter().collect(),
                attributes_int: item.attributes.attributes_int.into_iter().collect(),
                #(
                attributes_string_~N: item.attributes.attributes_string_~N.into_iter().collect(),
                attributes_float_~N: item.attributes.attributes_float_~N.into_iter().collect(),
                )*
                attributes_array: serde_json::to_string(&item.attributes.attributes_array).unwrap_or_default(),
            };
        }
    }
}

#[cfg(test)]
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
        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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
        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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
        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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

        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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

        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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

        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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

        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
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
        let rb_batch = process_message_row_binary(rb_payload, meta, &ProcessorConfig::default())
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
        let json_field = format!("attributes_string_{}", str_bucket);
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
            "str_attr not found in RowBinary output bucket {}",
            str_bucket
        );
        assert_eq!(
            json_str_map.get("str_attr"),
            rb_str_val,
            "str_attr differs between JSON and RowBinary"
        );

        // Verify float attribute is in the correct bucket (int_attr and bool_attr are
        // double-written as floats)
        let float_bucket = (fnv_1a("float_attr".as_bytes()) as usize) % 40;
        let json_float_field = format!("attributes_float_{}", float_bucket);
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

    /// Integration test that actually inserts an EAPItemRow into ClickHouse
    /// via RowBinary and reads it back. Requires ClickHouse with migrations applied.
    /// Runs in CI (where snuba-test-rust runs migrations first) but skipped locally
    /// unless explicitly requested with --include-ignored.
    #[tokio::test]
    #[ignore]
    async fn test_row_binary_clickhouse_insert() {
        let host = std::env::var("CLICKHOUSE_HOST").unwrap_or("127.0.0.1".to_string());
        let http_port: u16 = std::env::var("CLICKHOUSE_HTTP_PORT")
            .unwrap_or("8123".to_string())
            .parse()
            .unwrap();
        let database = std::env::var("CLICKHOUSE_DATABASE").unwrap_or("default".to_string());

        let client = clickhouse::Client::default()
            .with_url(format!("http://{}:{}", host, http_port))
            .with_database(&database)
            .with_option("input_format_binary_read_json_as_string", "1")
            .with_option("insert_deduplicate", "0");

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

        let batch = process_message_row_binary(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        assert_eq!(batch.rows.len(), 1);
        let row = batch.rows[0].clone();

        // Insert via RowBinary (same code path as production)
        let mut insert = client
            .insert("eap_items_1_local")
            .expect("Failed to create insert");
        insert.write(&row).await.expect("Failed to write row");
        insert.end().await.expect("Failed to end insert");

        // Read it back using organization_id (primary key prefix) for reliable lookup
        let count: u64 = client
            .query(&format!(
                "SELECT count() FROM eap_items_1_local WHERE organization_id = {}",
                unique_org_id
            ))
            .fetch_one()
            .await
            .expect("Failed to count rows");
        assert!(
            count > 0,
            "No rows found after insert for org_id={unique_org_id}"
        );

        let result = client
            .query(&format!(
                "SELECT organization_id, project_id, item_type, sampling_weight \
                 FROM eap_items_1_local \
                 WHERE organization_id = {} \
                 LIMIT 1",
                unique_org_id
            ))
            .fetch_one::<(u64, u64, u8, u64)>()
            .await
            .expect("Failed to read back inserted row");

        assert_eq!(result.0, unique_org_id); // organization_id
        assert_eq!(result.1, 1); // project_id
        assert_eq!(result.2, TraceItemType::Span as u8); // item_type
        assert_eq!(result.3, 1); // sampling_weight

        // Clean up
        client
            .query(&format!(
                "ALTER TABLE eap_items_1_local DELETE WHERE organization_id = {}",
                unique_org_id
            ))
            .execute()
            .await
            .ok();
    }
}
