use crate::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::str;
use thiserror::Error;

#[derive(Debug)]
struct Commit {
    topic: String,
    partition: u16,
    consumer_group: String,
    orig_message_ts: f64,
    offset: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Payload {
    offset: u64,
    orig_message_ts: f64,
}

#[derive(Error, Debug)]
enum CommitLogError {
    #[error("json error")]
    JsonError(#[from] serde_json::Error),
    #[error("invalid message key")]
    InvalidKey,
    #[error("invalid message payload")]
    InvalidPayload,
}

impl TryFrom<KafkaPayload> for Commit {
    type Error = CommitLogError;

    fn try_from(payload: KafkaPayload) -> Result<Self, CommitLogError> {
        let key = payload.key.unwrap();

        let data: Vec<&str> = str::from_utf8(&key).unwrap().split(':').collect();
        if data.len() != 3 {
            return Err(CommitLogError::InvalidKey);
        }

        let topic = data[0].to_string();
        let partition = data[1].parse::<u16>().unwrap();
        let consumer_group = data[2].to_string();

        let d: Payload =
            serde_json::from_slice(&payload.payload.ok_or(CommitLogError::InvalidPayload)?)?;

        Ok(Commit {
            topic,
            partition,
            consumer_group,
            orig_message_ts: d.orig_message_ts,
            offset: d.offset,
        })
    }
}

impl TryFrom<Commit> for KafkaPayload {
    type Error = CommitLogError;

    fn try_from(commit: Commit) -> Result<Self, CommitLogError> {
        let key = Some(
            format!(
                "{}:{}:{}",
                commit.topic, commit.partition, commit.consumer_group
            )
            .into_bytes(),
        );

        let payload = Some(serde_json::to_vec(&Payload {
            offset: commit.offset,
            orig_message_ts: commit.orig_message_ts,
        })?);

        Ok(KafkaPayload {
            key,
            headers: None,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit() {
        let payload = KafkaPayload {
            key: Some(b"topic:0:group1".to_vec()),
            headers: None,
            payload: Some(b"{\"offset\":5,\"orig_message_ts\":1696381946.0}".to_vec()),
        };

        let payload_clone = payload.clone();

        let commit: Commit = payload.try_into().unwrap();
        assert_eq!(commit.partition, 0);
        let transformed: KafkaPayload = commit.try_into().unwrap();
        assert_eq!(transformed.key, payload_clone.key);
        assert_eq!(transformed.payload, payload_clone.payload);
    }
}
