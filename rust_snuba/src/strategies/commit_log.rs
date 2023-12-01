use crate::types::BytesInsertBatch;
use chrono::{DateTime, NaiveDateTime, Utc};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::Producer;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::{Message, Topic, TopicOrPartition};
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
    orig_message_ts: DateTime<Utc>,
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
        let key = payload.key().unwrap();

        let data: Vec<&str> = str::from_utf8(key).unwrap().split(':').collect();
        if data.len() != 3 {
            return Err(CommitLogError::InvalidKey);
        }

        let topic = data[0].to_string();
        let partition = data[1].parse::<u16>().unwrap();
        let consumer_group = data[2].to_string();

        let d: Payload =
            serde_json::from_slice(payload.payload().ok_or(CommitLogError::InvalidPayload)?)?;

        let time_millis = (d.orig_message_ts * 1000.0) as i64;

        let orig_message_ts = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_millis(time_millis).unwrap_or(NaiveDateTime::MIN),
            Utc,
        );

        Ok(Commit {
            topic,
            partition,
            consumer_group,
            orig_message_ts,
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

        let orig_message_ts = commit.orig_message_ts.timestamp_millis() as f64 / 1000.0;

        let payload = Some(serde_json::to_vec(&Payload {
            offset: commit.offset,
            orig_message_ts,
        })?);

        Ok(KafkaPayload::new(key, None, payload))
    }
}

struct ProduceMessage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: Topic,
    destination: Arc<TopicOrPartition>,
    consumer_group: String,
    skip_produce: bool,
}

impl ProduceMessage {
    #[allow(dead_code)]
    pub fn new(
        producer: impl Producer<KafkaPayload> + 'static,
        topic: Topic,
        consumer_group: String,
        skip_produce: bool,
    ) -> Self {
        ProduceMessage {
            producer: Arc::new(producer),
            topic,
            destination: Arc::new(TopicOrPartition::Topic(topic)),
            consumer_group,
            skip_produce,
        }
    }
}

impl TaskRunner<BytesInsertBatch, BytesInsertBatch> for ProduceMessage {
    fn get_task(&self, message: Message<BytesInsertBatch>) -> RunTaskFunc<BytesInsertBatch> {
        let producer = self.producer.clone();
        let destination = self.destination.clone();
        let topic = self.topic.as_str().to_string();
        let skip_produce = self.skip_produce;
        let consumer_group = self.consumer_group.clone();

        let commit_log_offsets = message.payload().commit_log_offsets().clone();

        Box::pin(async move {
            if skip_produce {
                return Ok(message);
            }

            for (partition, (offset, orig_message_ts)) in commit_log_offsets {
                let commit = Commit {
                    topic: topic.clone(),
                    partition,
                    consumer_group: consumer_group.clone(),
                    orig_message_ts,
                    offset,
                };

                let payload = commit.try_into().unwrap();

                if let Err(err) = producer.produce(&destination, payload) {
                    tracing::error!(%err, "Error producing message");
                    return Err(RunTaskError::RetryableError);
                }
            }

            Ok(message)
        })
    }
}

pub struct ProduceCommitLog {
    inner: RunTaskInThreads<BytesInsertBatch, BytesInsertBatch>,
}

impl ProduceCommitLog {
    #[allow(dead_code)]
    pub fn new<N>(
        next_step: N,
        producer: impl Producer<KafkaPayload> + 'static,
        concurrency: &ConcurrencyConfig,
        topic: Topic,
        consumer_group: String,
        skip_produce: bool,
    ) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(ProduceMessage::new(
                producer,
                topic,
                consumer_group,
                skip_produce,
            )),
            concurrency,
            Some("produce_commit_log"),
        );

        ProduceCommitLog { inner }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ProduceCommitLog {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch>,
    ) -> Result<(), SubmitError<BytesInsertBatch>> {
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
    use crate::types::RowData;

    use super::*;
    use rust_arroyo::backends::ProducerError;
    use rust_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn commit() {
        let payload = KafkaPayload::new(
            Some(b"topic:0:group1".to_vec()),
            None,
            Some(b"{\"offset\":5,\"orig_message_ts\":1696381946.0}".to_vec()),
        );

        let payload_clone = payload.clone();

        let commit: Commit = payload.try_into().unwrap();
        assert_eq!(commit.partition, 0);
        let transformed: KafkaPayload = commit.try_into().unwrap();
        assert_eq!(transformed.key(), payload_clone.key());
        assert_eq!(transformed.payload(), payload_clone.payload());
    }

    #[test]
    fn produce_commit_log() {
        struct Noop {
            pub payloads: Vec<BytesInsertBatch>,
        }
        impl ProcessingStrategy<BytesInsertBatch> for Noop {
            fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
            fn submit(
                &mut self,
                message: Message<BytesInsertBatch>,
            ) -> Result<(), SubmitError<BytesInsertBatch>> {
                self.payloads.push(message.payload().clone());
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

        let produced_payloads = Arc::new(Mutex::new(Vec::new()));

        struct MockProducer {
            pub payloads: Arc<Mutex<Vec<KafkaPayload>>>,
        }

        impl Producer<KafkaPayload> for MockProducer {
            fn produce(
                &self,
                _topic: &TopicOrPartition,
                payload: KafkaPayload,
            ) -> Result<(), ProducerError> {
                self.payloads.lock().unwrap().push(payload);
                Ok(())
            }
        }

        let payloads = vec![
            BytesInsertBatch::new(
                RowData::from_rows(vec![]),
                Utc::now(),
                None,
                None,
                BTreeMap::from([(0, (500, Utc::now()))]),
            ),
            BytesInsertBatch::new(
                RowData::from_rows(vec![]),
                Utc::now(),
                None,
                None,
                BTreeMap::from([(0, (600, Utc::now())), (1, (100, Utc::now()))]),
            ),
        ];

        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };

        let next_step = Noop { payloads: vec![] };

        let concurrency = ConcurrencyConfig::new(1);
        let mut strategy = ProduceCommitLog::new(
            next_step,
            producer,
            &concurrency,
            Topic::new("test"),
            "group1".to_string(),
            false,
        );

        for payload in payloads {
            strategy
                .submit(Message::new_any_message(payload, BTreeMap::new()))
                .unwrap();
            strategy.poll().unwrap();
        }

        strategy.close();
        strategy.join(None).unwrap();

        assert_eq!(produced_payloads.lock().unwrap().len(), 3);
    }
}
