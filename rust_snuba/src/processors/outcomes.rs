use crate::config::ProcessorConfig;
use crate::processors::utils::StringToIntDatetime;
use crate::types::{InsertBatch, KafkaMessageMetadata};
use anyhow::Context;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const OUTCOME_ABUSE: u8 = 4;
const OUTCOME_CLIENT_DISCARD: u8 = 5;
// DataCategory 1 is Error
const DEFAULT_CATEGORY: u8 = 1;

const CLIENT_DISCARD_REASONS: &[&str] = &[
    // an event was dropped due to downsampling caused by the system being under load
    "backpressure",
    // an event was dropped in the `before_send` lifecycle method
    "before_send",
    // a SDK internal cache (eg: offline event cache) overflowed
    "cache_overflow",
    // an event was dropped by an event processor; may also be used for ignored exceptions / errors
    "event_processor",
    // an event was dropped due to a lack of data in the event (eg: not enough samples in a profile)
    "insufficient_data",
    // an event was dropped due to an internal SDK error (eg: web worker crash)
    "internal_sdk_error",
    // events were dropped because of network errors and were not retried.
    "network_error",
    // a SDK internal queue (eg: transport queue) overflowed
    "queue_overflow",
    // the SDK dropped events because an earlier rate limit instructed the SDK to back off.
    "ratelimit_backoff",
    // an event was dropped because of the configured sample rate.
    "sample_rate",
    // an event was dropped because of an error when sending it (eg: 400 response)
    "send_error",
    // an SDK internal buffer (eg. breadcrumbs buffer) overflowed
    "buffer_overflow",
];

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let mut msg: Outcome = serde_json::from_slice(payload_bytes)?;

    // relays let arbitrary outcome reasons through do the topic.  We
    // reject undesired values only in the processor so that we can
    // add new ones without having to update relays through the entire
    // chain.
    if msg.outcome == OUTCOME_CLIENT_DISCARD {
        if let Some(reason) = &msg.reason {
            if CLIENT_DISCARD_REASONS
                .binary_search(&reason.as_str())
                .is_err()
            {
                msg.reason = None;
            }
        }
    }

    // we dont care about abuse outcomes for these metrics
    if msg.category.is_none() {
        msg.category = Some(DEFAULT_CATEGORY);
        if msg.outcome != OUTCOME_ABUSE {
            counter!("missing_category");
        }
    }
    if msg.quantity.is_none() {
        msg.quantity = Some(1);
        if msg.outcome != OUTCOME_ABUSE {
            counter!("missing_quantity");
        }
    }

    InsertBatch::from_rows([msg], None)
}

#[derive(Debug, Deserialize, Serialize)]
struct Outcome {
    #[serde(default)]
    org_id: u64,
    #[serde(default)]
    project_id: Option<u64>,
    key_id: Option<u64>,
    #[serde(default)]
    timestamp: StringToIntDatetime,
    outcome: u8,
    category: Option<u8>,
    quantity: Option<u32>,
    reason: Option<String>,
    event_id: Option<Uuid>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use sentry_arroyo::backends::kafka::types::KafkaPayload;
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
        let result = process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");

        let expected = b"{\"org_id\":1,\"project_id\":1,\"key_id\":null,\"timestamp\":1680029444,\"outcome\":4,\"category\":1,\"quantity\":3,\"reason\":null,\"event_id\":null}\n";

        assert_eq!(result.rows.into_encoded_rows(), expected);
    }
}
