use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use prost::Message as ProstMessage;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message, Partition};
use sentry_protos::snuba::v1::TraceItem;

use crate::types::{AggregatedOutcomesBatch, BucketKey};

#[derive(Debug, Default)]
struct TraceItemOutcome {
    key_id: u64,
    category: u32,
    quantity: u64,
}

struct TraceItemOutcomes(Vec<TraceItemOutcome>);

impl TraceItemOutcomes {
    fn from_trace_item(item: TraceItem) -> Self {
        let mut outcomes = Vec::new();
        let Some(trace_outcomes) = item.outcomes else {
            return TraceItemOutcomes(vec![]);
        };

        let key_id = trace_outcomes.key_id;
        for cc in trace_outcomes.category_count {
            outcomes.push(TraceItemOutcome {
                key_id,
                category: cc.data_category,
                quantity: cc.quantity,
            });
        }
        TraceItemOutcomes(outcomes)
    }
}

pub struct OutcomesAggregator<TNext> {
    next_step: TNext,
    /// Maximum number of outcome buckets to accumulate before flushing.
    max_batch_size: usize,
    /// Seconds per time slot: offset = floor(event_ts_secs / bucket_interval)
    bucket_interval: u64,
    /// How long to accumulate before flushing
    max_batch_time_ms: Duration,
    last_flush: Instant,
    batch: AggregatedOutcomesBatch,
    /// Latest broker offset seen per partition across all buckets.
    latest_offsets: HashMap<Partition, u64>,
}

impl<TNext> OutcomesAggregator<TNext> {
    pub fn new(
        next_step: TNext,
        max_batch_size: usize,
        max_batch_time_ms: Duration,
        bucket_interval: u64,
    ) -> Self {
        Self {
            next_step,
            max_batch_size,
            max_batch_time_ms,
            bucket_interval,
            last_flush: Instant::now(),
            batch: AggregatedOutcomesBatch::new(bucket_interval),
            latest_offsets: HashMap::new(),
        }
    }

    fn flush(&mut self) -> Result<(), StrategyError>
    where
        TNext: ProcessingStrategy<AggregatedOutcomesBatch>,
    {
        let batch = std::mem::take(&mut self.batch);
        let latest_offsets = std::mem::take(&mut self.latest_offsets);

        // Committable offset is latest_offset + 1 (next offset to consume) per partition.
        let committable: BTreeMap<Partition, u64> = latest_offsets
            .iter()
            .map(|(partition, offset)| (partition.clone(), offset + 1))
            .collect();

        let message = Message::new_any_message(batch.clone(), committable);

        match self.next_step.submit(message) {
            Ok(()) => {
                // Keep the batch cleared only after a successful forward to next_step.
                self.last_flush = Instant::now();
                Ok(())
            }
            Err(SubmitError::MessageRejected(_)) => {
                tracing::warn!("Message rejected by CommitOutcomes during flush");
                self.batch = batch;
                self.latest_offsets = latest_offsets;
                Ok(())
            }
            Err(SubmitError::InvalidMessage(e)) => {
                self.batch = batch;
                self.latest_offsets = latest_offsets;
                Err(StrategyError::InvalidMessage(e))
            }
        }
    }
}

impl<TNext: ProcessingStrategy<AggregatedOutcomesBatch>> ProcessingStrategy<KafkaPayload>
    for OutcomesAggregator<TNext>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        if self.batch.num_buckets() >= self.max_batch_size
            || self.last_flush.elapsed() >= self.max_batch_time_ms
        {
            self.flush()?;
        }
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        let InnerMessage::BrokerMessage(ref broker_msg) = message.inner_message else {
            return Ok(());
        };

        let partition = broker_msg.partition.clone();
        let broker_offset = broker_msg.offset;

        self.latest_offsets
            .entry(partition)
            .and_modify(|o| *o = (*o).max(broker_offset))
            .or_insert(broker_offset);

        let Some(payload) = broker_msg.payload.payload() else {
            return Ok(());
        };

        let trace_item = match TraceItem::decode(payload as &[u8]) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed to decode TraceItem: {:?}", e);
                return Err(SubmitError::InvalidMessage(InvalidMessage::from(
                    broker_msg,
                )));
            }
        };

        let ts_secs = trace_item
            .received
            .as_ref()
            .map(|t| t.seconds as u64)
            .unwrap_or(0);
        let org_id = trace_item.organization_id;
        let project_id = trace_item.project_id;

        let TraceItemOutcomes(outcomes) = TraceItemOutcomes::from_trace_item(trace_item);
        for item in outcomes {
            let key = BucketKey {
                time_offset: ts_secs / self.bucket_interval,
                org_id,
                project_id,
                key_id: item.key_id,
                category: item.category,
            };
            self.batch.add_to_bucket(key, item.quantity);
        }

        Ok(())
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.flush()?;
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use prost::Message as ProstMessage;
    use prost_types::Timestamp;
    use sentry_arroyo::types::{Partition, Topic};
    use sentry_protos::snuba::v1::{CategoryCount, Outcomes};

    struct Noop {
        last_message: Option<Message<AggregatedOutcomesBatch>>,
    }
    impl ProcessingStrategy<AggregatedOutcomesBatch> for Noop {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
        fn submit(
            &mut self,
            message: Message<AggregatedOutcomesBatch>,
        ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
            self.last_message = Some(message);
            Ok(())
        }
        fn terminate(&mut self) {}
        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    fn make_payload(
        ts_secs: i64,
        org_id: u64,
        project_id: u64,
        key_id: u64,
        category_counts: &[(u32, u64)],
    ) -> KafkaPayload {
        let trace_item = TraceItem {
            organization_id: org_id,
            project_id,
            received: Some(Timestamp {
                seconds: ts_secs,
                nanos: 0,
            }),
            outcomes: Some(Outcomes {
                key_id,
                category_count: category_counts
                    .iter()
                    .map(|(data_category, quantity)| CategoryCount {
                        data_category: *data_category,
                        quantity: *quantity,
                    })
                    .collect(),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        trace_item.encode(&mut buf).unwrap();
        KafkaPayload::new(None, None, Some(buf))
    }

    #[test]
    fn submit_tracks_max_offset_per_partition() {
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(5_000),
            60,
        );

        let topic = Topic::new("accepted-outcomes");
        let partition_0 = Partition::new(topic.clone(), 0);
        let partition_1 = Partition::new(topic, 1);
        let payload = make_payload(1_700_000_000, 1, 2, 3, &[(4, 1)]);

        // Partition 0: offsets 2, 5, 10 → max = 10 → committable = 11
        for offset in [2, 5, 10] {
            aggregator
                .submit(Message::new_broker_message(
                    payload.clone(),
                    partition_0.clone(),
                    offset,
                    Utc::now(),
                ))
                .unwrap();
        }
        // Partition 1: offsets 3, 7, 15 → max = 15 → committable = 16
        for offset in [3, 7, 15] {
            aggregator
                .submit(Message::new_broker_message(
                    payload.clone(),
                    partition_1.clone(),
                    offset,
                    Utc::now(),
                ))
                .unwrap();
        }

        aggregator.flush().unwrap();

        let msg = aggregator.next_step.last_message.take().unwrap();
        let InnerMessage::AnyMessage(am) = msg.inner_message else {
            panic!("expected AnyMessage");
        };
        assert_eq!(am.committable.get(&partition_0).copied(), Some(11));
        assert_eq!(am.committable.get(&partition_1).copied(), Some(16));
    }

    #[test]
    fn submit_multiple_messages_accumulates_batch() {
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(2_000),
            60,
        );

        let topic = Topic::new("snuba-items");
        let partition = Partition::new(topic, 0);

        // Two submits land in the same minute bucket.
        aggregator
            .submit(Message::new_broker_message(
                make_payload(1_700_000_000, 1, 2, 3, &[(4, 7)]),
                partition.clone(),
                0,
                Utc::now(),
            ))
            .unwrap();
        aggregator
            .submit(Message::new_broker_message(
                make_payload(1_700_000_000, 1, 2, 3, &[(4, 3)]),
                partition.clone(),
                1,
                Utc::now(),
            ))
            .unwrap();
        // One minute later lands in a different bucket, plus has two category counts for the same message
        aggregator
            .submit(Message::new_broker_message(
                make_payload(1_700_000_060, 1, 2, 3, &[(4, 5), (7, 2)]),
                partition.clone(),
                2,
                Utc::now(),
            ))
            .unwrap();

        let key_1700000000 = BucketKey {
            time_offset: 28_333_333,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        let key_1700003600 = BucketKey {
            time_offset: 28_333_334,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        let key_1700003600_category_7 = BucketKey {
            time_offset: 28_333_334,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 7,
        };
        assert_eq!(aggregator.batch.num_buckets(), 3);
        assert_eq!(
            aggregator
                .batch
                .buckets
                .get(&key_1700000000)
                .map(|s| s.quantity),
            Some(10) // 7 + 3
        );
        assert_eq!(
            aggregator
                .batch
                .buckets
                .get(&key_1700003600)
                .map(|s| s.quantity),
            Some(5)
        );
        // separate batch for same timestamp because of category
        assert_eq!(
            aggregator
                .batch
                .buckets
                .get(&key_1700003600_category_7)
                .map(|s| s.quantity),
            Some(2)
        );
    }

    #[test]
    fn poll_flushes_when_max_batch_size_reached() {
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            1,
            Duration::from_millis(30_000),
            60,
        );

        let partition = Partition::new(Topic::new("accepted-outcomes"), 0);
        aggregator
            .submit(Message::new_broker_message(
                make_payload(6_000, 1, 2, 3, &[(4, 7)]),
                partition,
                0,
                Utc::now(),
            ))
            .unwrap();

        aggregator.poll().unwrap();
        assert_eq!(aggregator.batch.num_buckets(), 0);
    }
}
