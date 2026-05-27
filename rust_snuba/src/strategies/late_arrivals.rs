//! # ProduceLateArrivals
//!
//! Fans out messages emitted by a processor whose return type is
//! `InsertOrLateArrival<B>`. `Insert(batch)` is forwarded to the next step
//! (typically the Reduce/ClickHouse path). `LateArrival(payload)` carries the
//! original Kafka payload and is produced to a dedicated side topic — separate
//! from the regular DLQ — so it can be safely auto-replayed later without
//! mixing it with messages that are genuinely invalid.
//!
//! Modeled on `strategies::replacements::ProduceReplacements`.

use crate::strategies::noop::Noop;
use crate::types::InsertOrLateArrival;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::strategies::merge_commit_request;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};
use std::time::Duration;

pub struct ProduceLateArrivals<B> {
    next_step: Box<dyn ProcessingStrategy<B>>,
    inner: Box<dyn ProcessingStrategy<KafkaPayload>>,
    skip_produce: bool,
}

impl<B: Clone + Send + 'static> ProduceLateArrivals<B> {
    pub fn new<N>(
        next_step: N,
        producer: impl Producer<KafkaPayload> + 'static,
        destination: Topic,
        concurrency: &ConcurrencyConfig,
        skip_produce: bool,
    ) -> Self
    where
        N: ProcessingStrategy<B> + 'static,
    {
        let inner: Box<dyn ProcessingStrategy<KafkaPayload>> = if skip_produce {
            Box::new(Noop {})
        } else {
            Box::new(Produce::new(
                Noop {},
                producer,
                concurrency,
                TopicOrPartition::Topic(destination),
            ))
        };

        ProduceLateArrivals {
            next_step: Box::new(next_step),
            inner,
            skip_produce,
        }
    }

    /// Constructor for the case where the storage has not configured a
    /// `late_arrivals_topic`. Late arrivals are silently dropped (matching
    /// "killswitch disabled when no topic" semantics); inserts forward
    /// normally to `next_step`.
    pub fn disabled<N>(next_step: N) -> Self
    where
        N: ProcessingStrategy<B> + 'static,
    {
        ProduceLateArrivals {
            next_step: Box::new(next_step),
            inner: Box::new(Noop {}),
            skip_produce: true,
        }
    }
}

impl<B: Clone + Send + 'static> ProcessingStrategy<InsertOrLateArrival<B>>
    for ProduceLateArrivals<B>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        // Late-arrival offsets are not separately committed here — the inner
        // Produce strategy is wrapped in a Noop, mirroring ProduceReplacements.
        let _ = self.inner.poll();
        self.next_step.poll()
    }

    fn submit(
        &mut self,
        message: Message<InsertOrLateArrival<B>>,
    ) -> Result<(), SubmitError<InsertOrLateArrival<B>>> {
        let payload = message.clone().into_payload();

        match payload {
            InsertOrLateArrival::Insert(insert) => self
                .next_step
                .submit(message.clone().replace(insert))
                .map_err(|err| match err {
                    SubmitError::MessageRejected(message_rejected) => {
                        let payload = message_rejected.message.into_payload();
                        SubmitError::MessageRejected(MessageRejected {
                            message: message.replace(InsertOrLateArrival::Insert(payload)),
                        })
                    }
                    SubmitError::InvalidMessage(invalid) => SubmitError::InvalidMessage(invalid),
                }),
            InsertOrLateArrival::LateArrival(kafka_payload) => {
                if self.skip_produce {
                    tracing::info!("Skipping late-arrival message");
                    return Ok(());
                }

                self.inner
                    .submit(message.clone().replace(kafka_payload))
                    .map_err(|err| match err {
                        SubmitError::MessageRejected(message_rejected) => {
                            let payload = message_rejected.message.into_payload();
                            SubmitError::MessageRejected(MessageRejected {
                                message: message.replace(InsertOrLateArrival::LateArrival(payload)),
                            })
                        }
                        SubmitError::InvalidMessage(invalid) => {
                            SubmitError::InvalidMessage(invalid)
                        }
                    })
            }
        }
    }

    fn terminate(&mut self) {
        self.inner.terminate();
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        // Wait for all in-flight produces to complete before joining the next
        // step, to avoid committing offsets for a late arrival that hasn't
        // actually been written to the side topic yet.
        let committable = self.inner.join(None)?;
        Ok(merge_commit_request(
            committable,
            self.next_step.join(timeout)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::{MockProducer, TestStrategy};
    use crate::types::{BytesInsertBatch, InsertOrLateArrival, RowData};
    use chrono::Utc;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn forwards_insert_diverts_late_arrival() {
        let next_step = TestStrategy::new();
        let produced_payloads = Arc::new(Mutex::new(vec![]));
        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };
        let destination = Topic::new("late-arrivals");
        let concurrency = ConcurrencyConfig::new(10);

        let mut strategy: ProduceLateArrivals<BytesInsertBatch<RowData>> =
            ProduceLateArrivals::new(next_step, producer, destination, &concurrency, false);

        // Insert path: nothing should be produced to the late-arrivals topic.
        strategy
            .submit(Message::new_any_message(
                InsertOrLateArrival::Insert(
                    BytesInsertBatch::from_rows(RowData::from_rows(Vec::<u8>::new()).unwrap())
                        .with_message_timestamp(Utc::now()),
                ),
                BTreeMap::new(),
            ))
            .unwrap();
        assert_eq!(produced_payloads.lock().unwrap().len(), 0);

        // LateArrival path: the original payload should land on the late-arrivals topic.
        let payload_bytes = b"original-protobuf".to_vec();
        let key_bytes = b"k".to_vec();
        strategy
            .submit(Message::new_any_message(
                InsertOrLateArrival::LateArrival(KafkaPayload::new(
                    Some(key_bytes.clone()),
                    None,
                    Some(payload_bytes.clone()),
                )),
                BTreeMap::new(),
            ))
            .unwrap();

        strategy.poll().unwrap();
        strategy.join(None).unwrap();

        let produced = produced_payloads.lock().unwrap();
        assert_eq!(produced.len(), 1);
        assert_eq!(produced[0].1.payload(), Some(&payload_bytes));
    }
}
