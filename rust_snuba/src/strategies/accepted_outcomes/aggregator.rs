use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use prost::Message as ProstMessage;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message, Partition};
use sentry_protos::snuba::v1::{TraceItem, TraceItemType};

use crate::types::{AggregatedOutcomesBatch, BucketKey};

#[derive(Debug, Default)]
struct TraceItemOutcome {
    key_id: u64,
    category: u32,
    quantity: u64,
}

struct TraceItemOutcomes(Vec<TraceItemOutcome>);

impl TryFrom<TraceItem> for TraceItemOutcomes {
    type Error = anyhow::Error;

    fn try_from(from: TraceItem) -> Result<Self, Self::Error> {
        let mut outcomes = Vec::new();
        let Some(trace_outcomes) = from.outcomes else {
            return Ok(TraceItemOutcomes(vec![]));
        };

        let key_id = trace_outcomes.key_id;
        for cc in trace_outcomes.category_count {
            outcomes.push(TraceItemOutcome {
                key_id,
                category: cc.data_category,
                quantity: cc.quantity,
            });
        }
        Ok(TraceItemOutcomes(outcomes))
    }
}

pub struct OutcomesAggregator<TNext> {
    next_step: TNext,
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
    pub fn new(next_step: TNext, bucket_interval: u64, max_batch_time_ms: Duration) -> Self {
        Self {
            next_step,
            bucket_interval,
            max_batch_time_ms,
            last_flush: Instant::now(),
            batch: AggregatedOutcomesBatch::new(bucket_interval),
            latest_offsets: HashMap::new(),
        }
    }

    fn flush(&mut self) -> Result<(), StrategyError>
    where
        TNext: ProcessingStrategy<AggregatedOutcomesBatch>,
    {
        let num_buckets = self.batch.num_buckets();

        if num_buckets > 0 {
            // todo: do metrics for quanitites and category
            tracing::info!("Flushing {} outcome bucket(s)", num_buckets);
        }

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
        if self.last_flush.elapsed() >= self.max_batch_time_ms {
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
                return Ok(());
            }
        };

        let ts_secs = trace_item
            .received
            .as_ref()
            .map(|t| t.seconds as u64)
            .unwrap_or(0);
        let org_id = trace_item.organization_id;
        let project_id = trace_item.project_id;

        match TraceItemOutcomes::try_from(trace_item) {
            Ok(outcomes) => {
                for item in outcomes.0 {
                    let key = BucketKey {
                        time_offset: ts_secs / self.bucket_interval,
                        org_id,
                        project_id,
                        key_id: item.key_id,
                        category: item.category,
                    };
                    self.batch.add_to_bucket(key, item.quantity);
                }
            }
            Err(e) => {
                tracing::error!("Failed to parse outcomes from TraceItem: {:?}", e);
            }
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

    struct Noop;
    impl ProcessingStrategy<AggregatedOutcomesBatch> for Noop {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
        fn submit(
            &mut self,
            _: Message<AggregatedOutcomesBatch>,
        ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
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
        let mut aggregator = OutcomesAggregator::new(Noop, 60, Duration::from_millis(5_000));

        let partition = Partition::new(Topic::new("accepted-outcomes"), 0);
        let invalid_payload = KafkaPayload::new(None, None, Some(vec![0, 1, 2]));

        aggregator
            .submit(Message::new_broker_message(
                invalid_payload.clone(),
                partition.clone(),
                2,
                Utc::now(),
            ))
            .unwrap();
        aggregator
            .submit(Message::new_broker_message(
                invalid_payload,
                partition.clone(),
                10,
                Utc::now(),
            ))
            .unwrap();
    }

    #[test]
    fn submit_multiple_messages_accumulates_batch() {
        let mut aggregator = OutcomesAggregator::new(Noop, 60, Duration::from_millis(2_000));

        let topic = Topic::new("snuba-items");
        let partition = Partition::new(topic, 0);

        // ts_secs=6000 → time_offset = 6000/60 = 100; two separate submits for the same bucket
        aggregator
            .submit(Message::new_broker_message(
                make_payload(6_000, 1, 2, 3, &[(4, 7)]),
                partition.clone(),
                0,
                Utc::now(),
            ))
            .unwrap();
        aggregator
            .submit(Message::new_broker_message(
                make_payload(6_000, 1, 2, 3, &[(4, 3)]),
                partition.clone(),
                1,
                Utc::now(),
            ))
            .unwrap();
        // Different bucket: ts_secs=7200 → time_offset = 7200/60 = 120
        aggregator
            .submit(Message::new_broker_message(
                make_payload(7_200, 1, 2, 3, &[(4, 5)]),
                partition.clone(),
                2,
                Utc::now(),
            ))
            .unwrap();

        let key_100 = BucketKey {
            time_offset: 100,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        let key_120 = BucketKey {
            time_offset: 120,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        assert_eq!(aggregator.batch.num_buckets(), 2);
        assert_eq!(
            aggregator.batch.buckets.get(&key_100).map(|s| s.quantity),
            Some(10) // 7 + 3
        );
        assert_eq!(
            aggregator.batch.buckets.get(&key_120).map(|s| s.quantity),
            Some(5)
        );
    }
}
