use crate::processors::utils::ensure_valid_datetime;
use crate::types::{InsertBatch, KafkaMessageMetadata};
use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::utils::metrics::get_metrics;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const OUTCOME_ABUSE: u8 = 4;
const OUTCOME_CLIENT_DISCARD: u8 = 5;
// DataCategory 1 is Error
const DEFAULT_CATEGORY: u8 = 1;

const CLIENT_DISCARD_REASONS: [&str; 11] = [
    "queue_overflow",
    "cache_overflow",
    "ratelimit_backoff",
    "network_error",
    "before_send",
    "event_processor",
    "sample_rate",
    "send_error",
    "internal_sdk_error",
    "insufficient_data",
    "backpressure",
];

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let mut msg: Outcome = serde_json::from_slice(payload_bytes)?;

    // relays let arbitrary outcome reasons through do the topic.  We
    // reject undesired values only in the processor so that we can
    // add new ones without having to update relays through the entire
    // chain.
    if msg.outcome == OUTCOME_CLIENT_DISCARD {
        if let Some(reason) = &msg.reason {
            if !CLIENT_DISCARD_REASONS.contains(&reason.as_str()) {
                msg.reason = None;
            }
        }
    }

    if msg.outcome != OUTCOME_ABUSE {
        // we dont care about abuse outcomes for these metrics
        if msg.category.is_none() {
            get_metrics().increment("missing_category", 1, None);
            msg.category = Some(DEFAULT_CATEGORY);
        }
        if msg.quantity.is_none() {
            get_metrics().increment("missing_quantity", 1, None);
            msg.quantity = Some(1);
        }
    }

    InsertBatch::from_rows([msg])
}

#[derive(Debug, Deserialize, Serialize)]
struct Outcome {
    #[serde(default)]
    org_id: u64,
    #[serde(default)]
    project_id: Option<u64>,
    key_id: Option<u64>,
    #[serde(default, deserialize_with = "ensure_valid_datetime")]
    timestamp: u32,
    outcome: u8,
    category: Option<u8>,
    quantity: Option<u32>,
    reason: Option<String>,
    event_id: Option<Uuid>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RowData;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;
    #[test]
    fn test_outcome() {
        let data = r#"{
            "org_id": 1,
            "outcome": 4,
            "project_id": 1,
            "quantity": 3,
            "timestamp": "2023-03-28T18:50:44.000011Z"
          }"#;
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        let result = process_message(payload, meta).expect("The message should be processed");

        let expected = "{\"timestamp\":1680029444,\"org_id\":1,\"project_id\":1,\"key_id\":null,\"outcome\":4,\"reason\":null,\"event_id\":null,\"quantity\":3,\"category\":1}";

        assert_eq!(
            result.rows,
            RowData::from_rows(vec![expected.as_bytes().to_vec()]).unwrap()
        );
    }
}
