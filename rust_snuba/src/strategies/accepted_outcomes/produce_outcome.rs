use crate::types::{AggregatedOutcomesBatch, TrackOutcome};
use chrono::{DateTime, SecondsFormat, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::timer;
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Number of messages to produce before pausing to avoid flooding the producer queue.
const THROTTLE_BATCH_SIZE: u64 = 20_000;
/// How long to pause once THROTTLE_BATCH_SIZE messages have been produced.
const THROTTLE_SLEEP: Duration = Duration::from_millis(500);

struct ProduceOutcome {
    producer: Arc<dyn Producer<KafkaPayload>>,
    destination: Topic,
    throttle_batch_size: u64,
    throttle_sleep: Duration,
}

impl ProduceOutcome {
    pub fn new(producer: Arc<dyn Producer<KafkaPayload> + 'static>, destination: Topic) -> Self {
        ProduceOutcome {
            producer,
            destination,
            throttle_batch_size: THROTTLE_BATCH_SIZE,
            throttle_sleep: THROTTLE_SLEEP,
        }
    }
}

impl TaskRunner<AggregatedOutcomesBatch, AggregatedOutcomesBatch, anyhow::Error>
    for ProduceOutcome
{
    fn get_task(
        &self,
        message: Message<AggregatedOutcomesBatch>,
    ) -> RunTaskFunc<AggregatedOutcomesBatch, anyhow::Error> {
        let producer = self.producer.clone();
        let destination: TopicOrPartition = self.destination.into();
        let throttle_batch_size = self.throttle_batch_size;
        let throttle_sleep = self.throttle_sleep;

        Box::pin(async move {
            let produce_start = SystemTime::now();

            let bucket_interval = message.payload().bucket_interval;
            let mut produced_count: u64 = 0;
            for (key, stats) in &message.payload().buckets {
                let ts_secs = key.time_offset * bucket_interval;
                let timestamp = if ts_secs == 0 {
                    Utc::now()
                } else {
                    DateTime::from_timestamp(ts_secs as i64, 0).unwrap_or_else(Utc::now)
                };
                // convert to string with fractional seconds e.g.  "2019-09-29T09:46:40.000000Z"
                let timestamp_str = timestamp.to_rfc3339_opts(SecondsFormat::Micros, true);
                let outcome = TrackOutcome {
                    timestamp: timestamp_str,
                    org_id: key.org_id,
                    project_id: key.project_id,
                    key_id: key.key_id,
                    // always ACCEPTED outcome
                    outcome: 0,
                    category: key.category,
                    quantity: stats.quantity,
                };

                let payload_bytes = serde_json::to_vec(&outcome).map_err(|e| {
                    RunTaskError::Other(anyhow::anyhow!("serialization error: {e}"))
                })?;
                let payload = KafkaPayload::new(None, None, Some(payload_bytes));

                if let Err(err) = producer.produce(&destination, payload) {
                    let error: &dyn std::error::Error = &err;
                    tracing::error!(error, "Error producing outcome");
                    return Err(RunTaskError::RetryableError);
                }

                // Pace produces to avoid flooding the producer queue with a single
                // large batch all at once.
                produced_count += 1;
                if produced_count.is_multiple_of(throttle_batch_size) {
                    tokio::time::sleep(throttle_sleep).await;
                }
            }

            let produce_finish = SystemTime::now();
            if let Ok(elapsed) = produce_finish.duration_since(produce_start) {
                timer!("accepted_outcomes.batch_produce_ms", elapsed);
            }
            message
                .payload()
                .record_message_latency("produce_broker_latency");
            Ok(message)
        })
    }
}

pub struct ProduceAcceptedOutcome<N> {
    inner: RunTaskInThreads<AggregatedOutcomesBatch, AggregatedOutcomesBatch, anyhow::Error, N>,
}

impl<N> ProduceAcceptedOutcome<N>
where
    N: ProcessingStrategy<AggregatedOutcomesBatch> + 'static,
{
    pub fn new(
        next_step: N,
        producer: Arc<dyn Producer<KafkaPayload> + 'static>,
        destination: Topic,
        concurrency: &ConcurrencyConfig,
    ) -> Self {
        let inner = RunTaskInThreads::new(
            next_step,
            ProduceOutcome::new(producer, destination),
            concurrency,
            Some("produce_outcome"),
        );

        ProduceAcceptedOutcome { inner }
    }
}

impl<N> ProcessingStrategy<AggregatedOutcomesBatch> for ProduceAcceptedOutcome<N>
where
    N: ProcessingStrategy<AggregatedOutcomesBatch>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<AggregatedOutcomesBatch>,
    ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
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
    use super::*;
    use crate::strategies::noop::Noop;
    use crate::types::{BucketKey, BucketStats};
    use sentry_arroyo::backends::ProducerError;
    use sentry_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn produce_accepted_outcomes() {
        let produced_payloads = Arc::new(Mutex::new(Vec::new()));

        struct MockProducer {
            pub payloads: Arc<Mutex<Vec<KafkaPayload>>>,
        }

        impl Producer<KafkaPayload> for MockProducer {
            fn produce(
                &self,
                topic: &TopicOrPartition,
                payload: KafkaPayload,
            ) -> Result<(), ProducerError> {
                assert_eq!(topic.topic().as_str(), "test-outcomes");
                self.payloads.lock().unwrap().push(payload);
                Ok(())
            }
        }

        let mut batch = AggregatedOutcomesBatch::new(60);
        batch.buckets.insert(
            BucketKey {
                time_offset: 100,
                org_id: 1,
                project_id: 100,
                key_id: 10,
                category: 1,
            },
            BucketStats::new(5),
        );
        batch.buckets.insert(
            BucketKey {
                time_offset: 100,
                org_id: 2,
                project_id: 200,
                key_id: 20,
                category: 2,
            },
            BucketStats::new(10),
        );

        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };

        let concurrency = ConcurrencyConfig::new(1);
        let mut strategy = ProduceAcceptedOutcome::new(
            Noop,
            Arc::new(producer),
            Topic::new("test-outcomes"),
            &concurrency,
        );

        strategy
            .submit(Message::new_any_message(batch, BTreeMap::new()))
            .unwrap();
        strategy.poll().unwrap();
        strategy.join(None).unwrap();

        let produced = produced_payloads.lock().unwrap();
        assert_eq!(produced.len(), 2);
    }

    #[tokio::test]
    async fn throttles_large_batches() {
        use std::time::Instant;

        let produced_payloads = Arc::new(Mutex::new(Vec::new()));

        struct MockProducer {
            payloads: Arc<Mutex<Vec<KafkaPayload>>>,
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

        // Build a batch with 5 distinct buckets.
        let num_buckets: u64 = 5;
        let mut batch = AggregatedOutcomesBatch::new(60);
        for i in 0..num_buckets {
            batch.buckets.insert(
                BucketKey {
                    time_offset: 100,
                    org_id: 1,
                    project_id: 100,
                    key_id: i,
                    category: 1,
                },
                BucketStats::new(1),
            );
        }

        // Throttle after every 2 messages, sleeping 50ms each time. With 5
        // messages the sleep fires after the 2nd and 4th produce -> 2 sleeps.
        let throttle_batch_size = 2;
        let throttle_sleep = Duration::from_millis(100);
        let task_runner = ProduceOutcome {
            producer: Arc::new(MockProducer {
                payloads: produced_payloads.clone(),
            }),
            destination: Topic::new("test-outcomes"),
            throttle_batch_size,
            throttle_sleep,
        };

        let start = Instant::now();
        task_runner
            .get_task(Message::new_any_message(batch, BTreeMap::new()))
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // All buckets were produced.
        assert_eq!(
            produced_payloads.lock().unwrap().len(),
            num_buckets as usize
        );

        // Two throttle pauses should have elapsed so expected_time
        // should be at least 200ms
        let expected_time = Duration::from_millis(200);
        assert!(
            elapsed >= expected_time,
            "expected at least {expected_time:?} of throttling, got {elapsed:?}"
        );
    }

    #[test]
    fn test_timestamp_formatting() {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0).expect("expect timestamp");
        assert_eq!(
            timestamp.to_rfc3339_opts(SecondsFormat::Micros, true),
            "2023-11-14T22:13:20.000000Z"
        );
    }
}
