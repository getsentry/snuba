use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::str;

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

impl From<KafkaPayload> for Commit {
    fn from(payload: KafkaPayload) -> Self {
        let key = payload.key.unwrap();

        let data: Vec<&str> = str::from_utf8(&key).unwrap().split(':').collect();
        assert!(data.len() == 3);

        let topic = data[0].to_string();
        let partition = data[1].parse::<u16>().unwrap();
        let consumer_group = data[2].to_string();

        let d: Payload = serde_json::from_slice(&payload.payload.unwrap()).unwrap();

        Commit {
            topic,
            partition,
            consumer_group,
            orig_message_ts: d.orig_message_ts,
        }
    }
}

impl From<Commit> for KafkaPayload {
    fn from(commit: Commit) -> Self {
        let key = Some(
            format!(
                "{}:{}:{}",
                commit.topic, commit.partition, commit.consumer_group
            )
            .into_bytes(),
        );

        let payload = Some(serde_json::to_vec(&commit).unwrap());

        KafkaPayload {
            key,
            headers: None,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Produce;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
    use crate::types::{BrokerMessage, InnerMessage};
    use crate::types::{Message, Partition, Topic, TopicOrPartition};
    use chrono::Utc;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::Duration;

    #[test]
    fn commit() {
        let payload = KafkaPayload {
            key: Some(b"topic:0:group1"),
            headers: None,
            payload: Some(b"{'offset': 5, 'orig_message_ts': '2023-09-26T21:58:14.191325Z'}"),
        };

        let commit: Commit = payload.into();
        assert_eq!(commit.partition.index, 0);
        assert_eq!(commit.into(), payload);
    }
}
