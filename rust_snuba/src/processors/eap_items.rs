use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use seq_macro::seq;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_protos::snuba::v1::any_value::Value;
use sentry_protos::snuba::v1::TraceItem;

use crate::config::ProcessorConfig;
use crate::processors::utils::enforce_retention;
use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    msg: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
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
    let mut eap_item = EAPItem::try_from(trace_item)?;

    eap_item.retention_days = retention_days;
    eap_item.downsampled_retention_days = downsampled_retention_days;
    eap_item.attributes.insert_int(
        "sentry._internal.ingested_at".into(),
        Utc::now().timestamp_millis(),
    );

    InsertBatch::from_rows([eap_item], origin_timestamp)
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
            client_sample_rate: from.client_sample_rate,
            server_sample_rate: from.server_sample_rate,
        };

        for (key, value) in from.attributes {
            match value.value {
                Some(Value::StringValue(string)) => eap_item.attributes.insert_string(key, string),
                Some(Value::DoubleValue(double)) => eap_item.attributes.insert_float(key, double),
                Some(Value::IntValue(int)) => eap_item.attributes.insert_int(key, int),
                Some(Value::BoolValue(bool)) => eap_item.attributes.insert_bool(key, bool),
                Some(Value::BytesValue(_)) => (),
                Some(Value::ArrayValue(_)) => (),
                Some(Value::KvlistValue(_)) => (),
                None => (),
            }
        }

        if from.client_sample_rate > 0.0 {
            eap_item.sampling_factor *= from.client_sample_rate;
        }

        if from.server_sample_rate > 0.0 {
            eap_item.sampling_factor *= from.server_sample_rate;
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

seq_attrs! {
#[derive(Debug, Default, Serialize)]
pub(crate) struct AttributeMap {
    attributes_bool: HashMap<String, bool>,
    attributes_int: HashMap<String, i64>,
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
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use prost_types::Timestamp;
    use sentry_protos::snuba::v1::TraceItemType;
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
}
