use std::collections::HashSet;

use crate::types::{BadMessage, BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::Deserialize;

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
) -> Result<BytesInsertBatch, BadMessage> {
    if let Some(payload_bytes) = payload.payload {
        let msg: FromOutcomeMessage = serde_json::from_slice(&payload_bytes).map_err(|err| {
            log::error!("Failed to deserialize message: {}", err);
            BadMessage
        })?;

        return Ok(BytesInsertBatch { rows: vec![] });
    }

    Err(BadMessage)
}

#[derive(Debug, Default, Deserialize)]
struct FromOutcomeMessage {
    timestamp: String,

    org_id: Option<u64>,
    project_id: Option<u64>,
    key_id: Option<u64>,
    outcome: u8,

    reason: Option<String>,
    event_id: Option<String>,
    category: Option<u8>,
    quantity: Option<u64>,
}

struct Outcome {
    timestamp: u64,

    org_id: u64,
    project_id: u64,
    key_id: Option<u64>,
    outcome: u8,

    reason: Option<String>,
    event_id: String,
    category: u8,
    quantity: u64,
}

impl TryFrom<FromOutcomeMessage> for Outcome {
    type Error = BadMessage;

    fn try_from(from: FromOutcomeMessage) -> Result<Self, Self::Error> {
        const OUTCOME_ABUSE: u8 = 4;
        const OUTCOME_CLIENT_DISCARD: u8 = 5;
        let client_discard_reasons: HashSet<&str> = vec![
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
        ]
        .into_iter()
        .collect();

        let mut reason: Option<String> = from.reason;

        if let Some(real_reason) = reason {
            if !client_discard_reasons.contains(&real_reason.as_str())
                && from.outcome == OUTCOME_CLIENT_DISCARD
            {
                reason = None;
            }
        }

        Ok(Self {
            timestamp: todo!(),
            org_id: from.org_id.unwrap_or_default(),
            project_id: from.project_id.unwrap_or_default(),
            key_id: from.key_id,
            outcome: from.outcome,
            reason: reason,
            event_id: todo!(),
            category: from.category.unwrap_or(1), // Use DataCategory.ERROR instead
            quantity: from.quantity.unwrap_or(1),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;
    #[test]
    fn test_profile() {
        let data = r#"{
            "org_id": 1,
            "outcome": 4,
            "project_id": 1,
            "quantity": 3,
            "timestamp": "2023-03-28T18:50:49.442341621Z"
          }"#;
        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some(data.as_bytes().to_vec()),
        };
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
