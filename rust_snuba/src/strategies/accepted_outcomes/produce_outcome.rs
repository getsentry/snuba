use crate::types::{AggregatedOutcomesBatch, TrackOutcome};
use chrono::{DateTime, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

struct ProduceOutcome {
    producer: Arc<dyn Producer<KafkaPayload>>,
    destination: Topic,
    skip_produce: bool,
}

impl ProduceOutcome {
    pub fn new(
        producer: Arc<dyn Producer<KafkaPayload> + 'static>,
        destination: Topic,
        skip_produce: bool,
    ) -> Self {
        ProduceOutcome {
            producer,
            destination,
            skip_produce,
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
        let skip_produce = self.skip_produce;

        Box::pin(async move {
            let mut category_metrics: BTreeMap<u32, (u64, u64)> = BTreeMap::new();

            let bucket_interval = message.payload().bucket_interval;
            for (key, stats) in &message.payload().buckets {
                let entry = category_metrics.entry(key.category).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += stats.quantity;

                let ts_secs = key.time_offset * bucket_interval;
                let timestamp =
                    DateTime::from_timestamp(ts_secs as i64, 0).unwrap_or_else(Utc::now);

                let outcome = TrackOutcome {
                    timestamp,
                    org_id: key.org_id,
                    project_id: key.project_id,
                    key_id: key.key_id,
                    // always ACCEPTED outcome
                    outcome: 0,
                    category: key.category,
                    quantity: stats.quantity,
                };

                if skip_produce {
                    continue;
                }

                let payload_bytes = serde_json::to_vec(&outcome).map_err(|e| {
                    RunTaskError::Other(anyhow::anyhow!("serialization error: {}", e))
                })?;
                let payload = KafkaPayload::new(None, None, Some(payload_bytes));

                if let Err(err) = producer.produce(&destination, payload) {
                    let error: &dyn std::error::Error = &err;
                    tracing::error!(error, "Error producing outcome");
                    return Err(RunTaskError::RetryableError);
                }
            }

            for (category, (bucket_count, total_quantity)) in &category_metrics {
                counter!(
                    "accepted_outcomes.bucket_count",
                    *bucket_count as i64,
                    "data_category" => category.to_string()
                );
                counter!(
                    "accepted_outcomes.total_quantity",
                    *total_quantity as i64,
                    "data_category" => category.to_string()
                );
            }

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
        skip_produce: bool,
    ) -> Self {
        let inner = RunTaskInThreads::new(
            next_step,
            ProduceOutcome::new(producer, destination, skip_produce),
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
            false,
        );

        strategy
            .submit(Message::new_any_message(batch, BTreeMap::new()))
            .unwrap();
        strategy.poll().unwrap();
        strategy.join(None).unwrap();

        let produced = produced_payloads.lock().unwrap();
        assert_eq!(produced.len(), 2);
    }

    #[test]
    fn skip_produce_does_not_produce() {
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

        let produced_payloads = Arc::new(Mutex::new(Vec::new()));

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

        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };

        let concurrency = ConcurrencyConfig::new(1);
        let mut strategy = ProduceAcceptedOutcome::new(
            Noop,
            Arc::new(producer),
            Topic::new("test-outcomes"),
            &concurrency,
            true, // skip_produce
        );

        strategy
            .submit(Message::new_any_message(batch, BTreeMap::new()))
            .unwrap();
        strategy.poll().unwrap();
        strategy.join(None).unwrap();

        let produced = produced_payloads.lock().unwrap();
        assert_eq!(produced.len(), 0);
    }
}
