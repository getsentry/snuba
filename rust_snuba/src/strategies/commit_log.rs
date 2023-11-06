use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::Producer;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::{Message, TopicOrPartition};
use serde::{Deserialize, Serialize};
use std::str;
use std::sync::Arc;
use std::time::Duration;
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

struct ProduceMessage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: &'static TopicOrPartition,
    skip_produce: bool,
}

impl ProduceMessage {
    #[allow(dead_code)]
    pub fn new(
        producer: impl Producer<KafkaPayload> + 'static,
        topic: &'static TopicOrPartition,
        skip_produce: bool,
    ) -> Self {
        ProduceMessage {
            producer: Arc::new(producer),
            topic,
            skip_produce,
        }
    }
}

impl TaskRunner<KafkaPayload, KafkaPayload> for ProduceMessage {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<KafkaPayload> {
        let producer = self.producer.clone();
        let topic = self.topic;
        let skip_produce = self.skip_produce;

        Box::pin(async move {
            if skip_produce {
                return Ok(message);
            }

            match producer.produce(topic, message.payload()) {
                Ok(_) => Ok(message),
                Err(e) => {
                    log::error!("Error producing message: {}", e);
                    Err(RunTaskError::RetryableError)
                }
            }
        })
    }
}

pub struct ProduceCommitLog {
    inner: Box<dyn ProcessingStrategy<KafkaPayload>>,
}

impl ProduceCommitLog {
    #[allow(dead_code)]
    pub fn new<N>(
        next_step: N,
        producer: impl Producer<KafkaPayload> + 'static,
        concurrency: usize,
        topic: &'static TopicOrPartition,
        skip_produce: bool,
    ) -> Self
    where
        N: ProcessingStrategy<KafkaPayload> + 'static,
    {
        let inner = Box::new(RunTaskInThreads::new(
            next_step,
            Box::new(ProduceMessage::new(producer, topic, skip_produce)),
            concurrency,
            Some("produce_commit_log"),
        ));

        ProduceCommitLog { inner }
    }
}

impl ProcessingStrategy<KafkaPayload> for ProduceCommitLog {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        self.inner.submit(message)
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_arroyo::backends::ProducerError;
    use rust_arroyo::types::Topic;
    use std::collections::BTreeMap;

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

    #[test]
    fn produce_commit_log() {
        struct Noop {
            pub payloads: Vec<KafkaPayload>,
        }
        impl ProcessingStrategy<KafkaPayload> for Noop {
            fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
            fn submit(
                &mut self,
                message: Message<KafkaPayload>,
            ) -> Result<(), SubmitError<KafkaPayload>> {
                self.payloads.push(message.payload());
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(
                &mut self,
                _timeout: Option<Duration>,
            ) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
        }

        struct MockProducer {
            pub payloads: Vec<KafkaPayload>,
        }

        impl Producer<KafkaPayload> for MockProducer {
            fn produce(
                &self,
                _topic: &TopicOrPartition,
                payload: KafkaPayload,
            ) -> Result<(), ProducerError> {
                self.payloads.push(payload);
                Ok(())
            }
        }

        let payloads = vec![
            KafkaPayload {
                key: Some(b"topic:0:group1".to_vec()),
                headers: None,
                payload: Some(
                    b"{\"offset\":5,\"orig_message_ts\":100000.0,\"received_p99\":100000.0}"
                        .to_vec(),
                ),
            },
            KafkaPayload {
                key: Some(b"topic:0:group1".to_vec()),
                headers: None,
                payload: Some(
                    b"{\"offset\":6,\"orig_message_ts\":100001.0,\"received_p99\":100001.0}"
                        .to_vec(),
                ),
            },
        ];

        let producer = MockProducer { payloads: vec![] };

        let next_step = Noop { payloads: vec![] };

        let topic = TopicOrPartition::Topic(Topic {
            name: "test".to_string(),
        });

        let strategy = ProduceCommitLog::new(next_step, producer, 1, &topic, false);

        for payload in payloads {
            strategy.submit(Message::new_any_message(payload, BTreeMap::new()));
            strategy.poll();
        }

        assert_eq!(producer.payloads.len(), payloads.len());
    }
}
