use crate::types::BytesInsertBatch;
use chrono::{DateTime, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug)]
struct Commit {
    topic: String,
    group: String,
    partition: u16,
    offset: u64,
    orig_message_ts: DateTime<Utc>,
    received_p99: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Payload {
    offset: u64,
    orig_message_ts: f64,
    received_p99: Option<f64>,
}

#[derive(Error, Debug)]
enum CommitLogError {
    #[error("json error")]
    JsonError(#[from] serde_json::Error),
    #[cfg(test)]
    #[error("invalid message key")]
    InvalidKey,
    #[cfg(test)]
    #[error("invalid message payload")]
    InvalidPayload,
}

impl TryFrom<Commit> for KafkaPayload {
    type Error = CommitLogError;

    fn try_from(commit: Commit) -> Result<Self, CommitLogError> {
        let key =
            Some(format!("{}:{}:{}", commit.topic, commit.partition, commit.group).into_bytes());

        let orig_message_ts = commit.orig_message_ts.timestamp_millis() as f64 / 1000.0;
        let received_p99 = commit
            .received_p99
            .map(|t| t.timestamp_millis() as f64 / 1000.0);

        let payload = Some(serde_json::to_vec(&Payload {
            offset: commit.offset,
            orig_message_ts,
            received_p99,
        })?);

        Ok(KafkaPayload::new(key, None, payload))
    }
}

struct ProduceMessage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    destination: Topic,
    topic: Topic,
    consumer_group: String,
    skip_produce: bool,
    assigned_partitions: Vec<u16>,
    last_produced_offsets: Arc<Mutex<BTreeMap<u16, (u64, DateTime<Utc>)>>>,
}

impl ProduceMessage {
    pub fn new(
        producer: Arc<dyn Producer<KafkaPayload> + 'static>,
        destination: Topic,
        topic: Topic,
        consumer_group: String,
        skip_produce: bool,
        assigned_partitions: Vec<u16>,
    ) -> Self {
        ProduceMessage {
            producer,
            destination,
            topic,
            consumer_group,
            skip_produce,
            assigned_partitions,
            last_produced_offsets: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl TaskRunner<BytesInsertBatch<()>, BytesInsertBatch<()>, anyhow::Error> for ProduceMessage {
    fn get_task(
        &self,
        message: Message<BytesInsertBatch<()>>,
    ) -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        let producer = self.producer.clone();
        let destination: TopicOrPartition = self.destination.into();
        let topic = self.topic;
        let skip_produce = self.skip_produce;
        let consumer_group = self.consumer_group.clone();
        let assigned_partitions = self.assigned_partitions.clone();
        let last_produced_offsets = self.last_produced_offsets.clone();

        let commit_log_offsets = message.payload().commit_log_offsets().clone();

        Box::pin(async move {
            if skip_produce {
                return Ok(message);
            }

            let partitions_in_batch: std::collections::BTreeSet<u16> =
                commit_log_offsets.0.keys().copied().collect();

            for (partition, mut entry) in commit_log_offsets.0 {
                entry.received_p99.sort();
                let received_p99 = entry
                    .received_p99
                    .get((entry.received_p99.len() as f64 * 0.99) as usize)
                    .copied();
                let commit = Commit {
                    topic: topic.to_string(),
                    partition,
                    group: consumer_group.clone(),
                    orig_message_ts: entry.orig_message_ts,
                    offset: entry.offset,
                    received_p99,
                };

                let payload = commit.try_into().unwrap();

                if let Err(err) = producer.produce(&destination, payload) {
                    let error: &dyn std::error::Error = &err;
                    tracing::error!(error, "Error producing message");
                    return Err(RunTaskError::RetryableError);
                }

                // Update last produced offset for this partition
                last_produced_offsets
                    .lock()
                    .unwrap()
                    .insert(partition, (entry.offset, entry.orig_message_ts));
            }

            // Produce heartbeat entries for idle assigned partitions
            let offsets = last_produced_offsets.lock().unwrap().clone();
            for &partition in &assigned_partitions {
                if !partitions_in_batch.contains(&partition) {
                    if let Some(&(offset, orig_message_ts)) = offsets.get(&partition) {
                        let commit = Commit {
                            topic: topic.to_string(),
                            partition,
                            group: consumer_group.clone(),
                            orig_message_ts,
                            offset,
                            received_p99: None,
                        };

                        let payload = commit.try_into().unwrap();

                        if let Err(err) = producer.produce(&destination, payload) {
                            let error: &dyn std::error::Error = &err;
                            tracing::error!(error, "Error producing heartbeat message");
                            return Err(RunTaskError::RetryableError);
                        }
                    }
                }
            }

            Ok(message)
        })
    }
}

pub struct ProduceCommitLog<N> {
    inner: RunTaskInThreads<BytesInsertBatch<()>, BytesInsertBatch<()>, anyhow::Error, N>,
}

impl<N> ProduceCommitLog<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    pub fn new(
        next_step: N,
        producer: Arc<dyn Producer<KafkaPayload> + 'static>,
        destination: Topic,
        topic: Topic,
        consumer_group: String,
        concurrency: &ConcurrencyConfig,
        skip_produce: bool,
        assigned_partitions: Vec<u16>,
    ) -> Self {
        let inner = RunTaskInThreads::new(
            next_step,
            ProduceMessage::new(
                producer,
                destination,
                topic,
                consumer_group,
                skip_produce,
                assigned_partitions,
            ),
            concurrency,
            Some("produce_commit_log"),
        );

        ProduceCommitLog { inner }
    }
}

impl<N> ProcessingStrategy<BytesInsertBatch<()>> for ProduceCommitLog<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<()>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<()>>> {
        self.inner.submit(message)
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{CommitLogEntry, CommitLogOffsets};

    use super::*;
    use crate::testutils::TestStrategy;
    use sentry_arroyo::backends::ProducerError;
    use sentry_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    impl TryFrom<KafkaPayload> for Commit {
        type Error = CommitLogError;

        fn try_from(payload: KafkaPayload) -> Result<Self, CommitLogError> {
            let key = payload.key().unwrap();

            let data: Vec<&str> = str::from_utf8(key).unwrap().split(':').collect();
            if data.len() != 3 {
                return Err(CommitLogError::InvalidKey);
            }

            let topic = data[0].to_owned();
            let partition = data[1].parse::<u16>().unwrap();
            let consumer_group = data[2].to_owned();

            let d: Payload =
                serde_json::from_slice(payload.payload().ok_or(CommitLogError::InvalidPayload)?)?;

            let time_millis = (d.orig_message_ts * 1000.0) as i64;

            let orig_message_ts =
                DateTime::<Utc>::from_timestamp_millis(time_millis).unwrap_or_default();

            Ok(Commit {
                topic,
                partition,
                group: consumer_group,
                orig_message_ts,
                offset: d.offset,
                received_p99: None,
            })
        }
    }

    #[test]
    fn commit() {
        let payload = KafkaPayload::new(
            Some(b"topic:0:group1".to_vec()),
            None,
            Some(b"{\"offset\":5,\"orig_message_ts\":1696381946.0,\"received_p99\":null}".to_vec()),
        );

        let payload_clone = payload.clone();

        let commit: Commit = payload.try_into().unwrap();
        assert_eq!(commit.partition, 0);
        let transformed: KafkaPayload = commit.try_into().unwrap();
        assert_eq!(transformed.key(), payload_clone.key());
        assert_eq!(
            std::str::from_utf8(transformed.payload().unwrap()).unwrap(),
            std::str::from_utf8(payload_clone.payload().unwrap()).unwrap()
        );
    }

    #[test]
    fn produce_commit_log() {
        let produced_payloads = Arc::new(Mutex::new(Vec::new()));

        struct MockProducer {
            pub payloads: Arc<Mutex<Vec<(String, KafkaPayload)>>>,
        }

        impl Producer<KafkaPayload> for MockProducer {
            fn produce(
                &self,
                topic: &TopicOrPartition,
                payload: KafkaPayload,
            ) -> Result<(), ProducerError> {
                assert_eq!(topic.topic().as_str(), "test-commitlog");
                self.payloads.lock().unwrap().push((
                    str::from_utf8(payload.key().unwrap()).unwrap().to_owned(),
                    payload,
                ));
                Ok(())
            }
        }

        let payloads = vec![
            BytesInsertBatch::from_rows(())
                .with_message_timestamp(Utc::now())
                .with_commit_log_offsets(CommitLogOffsets(BTreeMap::from([(
                    0,
                    CommitLogEntry {
                        offset: 500,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                )]))),
            BytesInsertBatch::from_rows(())
                .with_message_timestamp(Utc::now())
                .with_commit_log_offsets(CommitLogOffsets(BTreeMap::from([
                    (
                        0,
                        CommitLogEntry {
                            offset: 600,
                            orig_message_ts: Utc::now(),
                            received_p99: Vec::new(),
                        },
                    ),
                    (
                        1,
                        CommitLogEntry {
                            offset: 100,
                            orig_message_ts: Utc::now(),
                            received_p99: Vec::new(),
                        },
                    ),
                ]))),
        ];

        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };

        let next_step = TestStrategy::new();

        let concurrency = ConcurrencyConfig::new(1);
        let mut strategy = ProduceCommitLog::new(
            next_step,
            Arc::new(producer),
            Topic::new("test-commitlog"),
            Topic::new("test"),
            "group1".to_string(),
            &concurrency,
            false,
            vec![0, 1],
        );

        for payload in payloads {
            strategy
                .submit(Message::new_any_message(payload, BTreeMap::new()))
                .unwrap();
            strategy.poll().unwrap();
        }

        strategy.join(None).unwrap();

        let produced = produced_payloads.lock().unwrap();

        assert_eq!(produced.len(), 3);
        assert_eq!(produced[0].0, "test:0:group1");
        assert_eq!(produced[1].0, "test:0:group1");
        assert_eq!(produced[2].0, "test:1:group1");
    }

    #[test]
    fn produce_commit_log_heartbeat_for_idle_partitions() {
        // Assigned partitions 0-3, but only partitions 0 and 1 have data.
        // After the first batch establishes offsets for partitions 2 and 3,
        // a second batch with only partitions 0 and 1 should still produce
        // heartbeat entries for partitions 2 and 3.
        let produced_payloads = Arc::new(Mutex::new(Vec::new()));

        struct MockProducer {
            pub payloads: Arc<Mutex<Vec<(String, KafkaPayload)>>>,
        }

        impl Producer<KafkaPayload> for MockProducer {
            fn produce(
                &self,
                topic: &TopicOrPartition,
                payload: KafkaPayload,
            ) -> Result<(), ProducerError> {
                assert_eq!(topic.topic().as_str(), "test-commitlog");
                self.payloads.lock().unwrap().push((
                    str::from_utf8(payload.key().unwrap()).unwrap().to_owned(),
                    payload,
                ));
                Ok(())
            }
        }

        // Batch 1: all 4 partitions have data (establishes last known offsets)
        let batch1 = BytesInsertBatch::from_rows(())
            .with_message_timestamp(Utc::now())
            .with_commit_log_offsets(CommitLogOffsets(BTreeMap::from([
                (
                    0,
                    CommitLogEntry {
                        offset: 100,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                ),
                (
                    1,
                    CommitLogEntry {
                        offset: 200,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                ),
                (
                    2,
                    CommitLogEntry {
                        offset: 300,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                ),
                (
                    3,
                    CommitLogEntry {
                        offset: 400,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                ),
            ])));

        // Batch 2: only partitions 0 and 1 have data
        let batch2 = BytesInsertBatch::from_rows(())
            .with_message_timestamp(Utc::now())
            .with_commit_log_offsets(CommitLogOffsets(BTreeMap::from([
                (
                    0,
                    CommitLogEntry {
                        offset: 500,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                ),
                (
                    1,
                    CommitLogEntry {
                        offset: 600,
                        orig_message_ts: Utc::now(),
                        received_p99: Vec::new(),
                    },
                ),
            ])));

        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };

        let next_step = TestStrategy::new();

        let concurrency = ConcurrencyConfig::new(1);
        let mut strategy = ProduceCommitLog::new(
            next_step,
            Arc::new(producer),
            Topic::new("test-commitlog"),
            Topic::new("test"),
            "group1".to_string(),
            &concurrency,
            false,
            vec![0, 1, 2, 3], // assigned partitions 0-3
        );

        // Submit and process batch 1
        strategy
            .submit(Message::new_any_message(batch1, BTreeMap::new()))
            .unwrap();
        strategy.poll().unwrap();

        // Submit and process batch 2
        strategy
            .submit(Message::new_any_message(batch2, BTreeMap::new()))
            .unwrap();
        strategy.poll().unwrap();

        strategy.join(None).unwrap();

        let produced = produced_payloads.lock().unwrap();

        // Batch 1: 4 entries (all partitions have data, no heartbeats needed)
        // Batch 2: 2 entries (partitions 0,1 data) + 2 heartbeats (partitions 2,3)
        assert_eq!(produced.len(), 8);

        // Batch 1 entries
        assert_eq!(produced[0].0, "test:0:group1");
        assert_eq!(produced[1].0, "test:1:group1");
        assert_eq!(produced[2].0, "test:2:group1");
        assert_eq!(produced[3].0, "test:3:group1");

        // Batch 2: partitions 0 and 1 (data), then heartbeats for 2 and 3
        assert_eq!(produced[4].0, "test:0:group1");
        assert_eq!(produced[5].0, "test:1:group1");
        assert_eq!(produced[6].0, "test:2:group1");
        assert_eq!(produced[7].0, "test:3:group1");

        // Verify heartbeat offsets match the last known offsets from batch 1
        let heartbeat_2: Commit = produced[6].1.clone().try_into().unwrap();
        assert_eq!(heartbeat_2.offset, 300);

        let heartbeat_3: Commit = produced[7].1.clone().try_into().unwrap();
        assert_eq!(heartbeat_3.offset, 400);
    }
}
