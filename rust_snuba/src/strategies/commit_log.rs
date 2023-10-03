use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::str;
use thiserror::Error;

#[derive(Debug, Serialize)]
struct Commit {
    topic: String,
    partition: u16,
    consumer_group: String,
    orig_message_ts: f64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Payload {
    offset: u64,
    orig_message_ts: f64,
}

#[derive(Error, Debug)]
#[error(transparent)]
enum CommitLogError {
    #[error("encode error")]
    EncodeError,
    #[error("decode error")]
    DecodeError,
}

impl TryFrom<KafkaPayload> for Commit {
    type Error = CommitLogError;

    fn try_from(payload: KafkaPayload) -> Result<Self, CommitLogError> {
        let key = payload.key.unwrap();

        let data: Vec<&str> = str::from_utf8(&key).unwrap().split(':').collect();
        if data.len() != 3 {
            return Err(CommitLogError::DecodeError);
        }

        let topic = data[0].to_string();
        let partition = data[1].parse::<u16>().unwrap();
        let consumer_group = data[2].to_string();

        let d: Payload =
            serde_json::from_slice(&payload.payload.ok_or(CommitLogError::DecodeError)?)
                .map_err(|_| CommitLogError::DecodeError)?;

        Ok(Commit {
            topic,
            partition,
            consumer_group,
            orig_message_ts: d.orig_message_ts,
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

        let payload = Some(serde_json::to_vec(&commit).map_err(|_| CommitLogError::EncodeError)?);

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
            payload: Some(
                b"{'offset': 5, 'orig_message_ts': '2023-09-26T21:58:14.191325Z'}".to_vec(),
            ),
        };

        let payload_clone = payload.clone();

        let commit: Commit = payload.try_into().unwrap();
        assert_eq!(commit.partition, 0);
        let transformed: KafkaPayload = commit.try_into().unwrap();
        assert_eq!(transformed.key, payload_clone.key);
        assert_eq!(transformed.payload, payload_clone.payload);
    }
}
