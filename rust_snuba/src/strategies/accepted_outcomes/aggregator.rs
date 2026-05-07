use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use prost::Message as ProstMessage;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message, Partition};
use sentry_arroyo::utils::timing::Deadline;
use sentry_protos::snuba::v1::{TraceItem, TraceItemType};

use sentry_options::options;

use crate::types::{item_type_name, AggregatedOutcomesBatch, BucketKey, ItemDedupKey};

const OPTIONS_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

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
    /// A message rejected by the next step, to be retried on the next poll.
    message_carried_over: Option<Message<AggregatedOutcomesBatch>>,
    /// Commit request carried over from a poll where we had a message to retry.
    commit_request_carried_over: Option<CommitRequest>,
    /// Cached value of the `consumer.use_item_timestamp` option, refreshed at most once per `OPTIONS_REFRESH_INTERVAL`.
    use_item_timestamp: bool,
    /// Cached value of the `consumer.log_duplicates` option, refreshed at most once per `OPTIONS_REFRESH_INTERVAL`.
    log_duplicates: bool,
    /// Last time cached options were refreshed; `None` means never refreshed yet.
    last_options_refresh: Option<Instant>,
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
            message_carried_over: None,
            commit_request_carried_over: None,
            use_item_timestamp: false,
            log_duplicates: false,
            last_options_refresh: None,
        }
    }

    fn refresh_options_if_stale(&mut self) {
        if self
            .last_options_refresh
            .is_some_and(|t| t.elapsed() < OPTIONS_REFRESH_INTERVAL)
        {
            return;
        }
        let snuba_opts = options("snuba").ok();
        self.use_item_timestamp = snuba_opts
            .as_ref()
            .and_then(|o| o.get("consumer.use_item_timestamp").ok())
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        self.log_duplicates = snuba_opts
            .as_ref()
            .and_then(|o| o.get("consumer.log_duplicates").ok())
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        self.last_options_refresh = Some(Instant::now());
    }

    fn is_duplicate(&mut self, trace_item: &TraceItem) -> bool {
        let org_id = trace_item.organization_id;
        let project_id = trace_item.project_id;
        let item_type =
            TraceItemType::try_from(trace_item.item_type).unwrap_or(TraceItemType::Unspecified);

        if item_type != TraceItemType::Span {
            return false;
        }

        let timestamp = trace_item
            .timestamp
            .as_ref()
            .map(|t| t.seconds as u32)
            .unwrap_or(0);

        let trace_id = uuid::Uuid::try_parse(&trace_item.trace_id)
            .map(|u| *u.as_bytes())
            .unwrap_or([0u8; 16]);

        if let Ok(item_id) = <[u8; 16]>::try_from(trace_item.item_id.as_slice()) {
            let dedup_key = ItemDedupKey {
                org_id,
                project_id,
                timestamp,
                trace_id,
                item_id,
            };
            let is_dup = self.batch.record_if_duplicate(item_type, dedup_key);
            if is_dup && self.log_duplicates {
                let item_type_str = item_type_name(item_type);
                let item_id_uuid = uuid::Uuid::from_bytes(item_id);
                tracing::info!(
                    "duplicate {} trace: org_id:{}, project_id:{}, item_id:{}",
                    item_type_str,
                    org_id,
                    project_id,
                    item_id_uuid
                );
            }
            return is_dup;
        }
        false
    }

    fn flush(&mut self) -> Result<(), StrategyError>
    where
        TNext: ProcessingStrategy<AggregatedOutcomesBatch>,
    {
        let num_buckets = self.batch.num_buckets();
        let batch = std::mem::replace(
            &mut self.batch,
            AggregatedOutcomesBatch::new(self.bucket_interval),
        );
        let latest_offsets = std::mem::take(&mut self.latest_offsets);

        // Committable offset is latest_offset + 1 (next offset to consume) per partition.
        let committable: BTreeMap<Partition, u64> = latest_offsets
            .iter()
            .map(|(partition, offset)| (*partition, offset + 1))
            .collect();

        let category_metrics = batch.category_metrics.clone();
        let duplicate_item_counts = batch.duplicate_item_count.clone();
        let message = Message::new_any_message(batch, committable);
        match self.next_step.submit(message) {
            Ok(()) => {
                let now = Instant::now();
                let seconds = (now - self.last_flush).as_secs_f64();
                self.last_flush = now;

                tracing::info!("flushed {} buckets after {} seconds", num_buckets, seconds);
                for (item_type, count) in duplicate_item_counts {
                    let item_type_str = item_type_name(item_type);
                    counter!("accepted_outcomes.duplicate_items", count, "item_type" => item_type_str.as_str());
                }
                for (category, m) in category_metrics {
                    let cat_str = category.to_string();
                    counter!("accepted_outcomes.messages_seen", m.messages_seen, "data_category" => cat_str.as_str());
                    counter!("accepted_outcomes.total_quantity", m.total_quantity, "data_category" => cat_str.as_str());
                    counter!("accepted_outcomes.bucket_count", m.bucket_count, "data_category" => cat_str.as_str());
                }
                Ok(())
            }
            Err(SubmitError::MessageRejected(rejected)) => {
                self.message_carried_over = Some(rejected.message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(e)) => Err(StrategyError::InvalidMessage(e)),
        }
    }
}

impl<TNext: ProcessingStrategy<AggregatedOutcomesBatch>> ProcessingStrategy<KafkaPayload>
    for OutcomesAggregator<TNext>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.refresh_options_if_stale();

        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        if let Some(msg) = self.message_carried_over.take() {
            match self.next_step.submit(msg) {
                Ok(()) => {}
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: carried_message,
                })) => {
                    counter!("accepted_outcomes.got_backpressure", 1, "strategy_name" => "outcomes_aggregator");
                    self.message_carried_over = Some(carried_message);
                }
                Err(SubmitError::InvalidMessage(e)) => {
                    return Err(StrategyError::InvalidMessage(e));
                }
            }
        }

        if self.message_carried_over.is_none()
            && (self.batch.num_buckets() >= self.max_batch_size
                || self.last_flush.elapsed() >= self.max_batch_time_ms)
        {
            self.flush()?;
        }

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let InnerMessage::BrokerMessage(ref broker_msg) = message.inner_message else {
            unreachable!("Unexpected message type");
        };

        let partition = broker_msg.partition;
        let broker_offset = broker_msg.offset;

        self.latest_offsets
            .entry(partition)
            .and_modify(|o| *o = (*o).max(broker_offset))
            .or_insert(broker_offset);

        let maybe_err = SubmitError::InvalidMessage(InvalidMessage {
            partition,
            offset: broker_offset,
        });

        let kafka_payload = &broker_msg.payload.clone();
        let Some(payload) = kafka_payload.payload() else {
            return Err(maybe_err);
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

        let ts_secs = if self.use_item_timestamp {
            trace_item
                .timestamp
                .as_ref()
                .or(trace_item.received.as_ref())
                .map(|t| t.seconds as u64)
                .unwrap_or(0)
        } else {
            trace_item
                .received
                .as_ref()
                .map(|t| t.seconds as u64)
                .unwrap_or(0)
        };

        let org_id = trace_item.organization_id;
        let project_id = trace_item.project_id;

        if self.is_duplicate(&trace_item) {
            return Ok(());
        }

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
        let deadline = timeout.map(Deadline::new);

        if self.message_carried_over.is_none() {
            self.flush()?;
        }

        while self.message_carried_over.is_some() {
            if deadline.is_some_and(|d| d.has_elapsed()) {
                tracing::warn!("Timeout reached while waiting for carried-over outcomes");
                break;
            }

            let commit_request = self.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);
        }

        let next_commit = self.next_step.join(deadline.map(|d| d.remaining()))?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use prost::Message as ProstMessage;
    use prost_types::Timestamp;
    use sentry_arroyo::types::{Partition, Topic};
    use sentry_options::init_with_schemas;
    use sentry_options::testing::override_options;
    use sentry_protos::snuba::v1::TraceItemType;
    use sentry_protos::snuba::v1::{CategoryCount, Outcomes};
    use serde_json::json;

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
        let partition_0 = Partition::new(topic, 0);
        let partition_1 = Partition::new(topic, 1);
        let payload = make_payload(1_700_000_000, 1, 2, 3, &[(4, 1)]);

        // Partition 0: offsets 2, 5, 10 → max = 10 → committable = 11
        for offset in [2, 5, 10] {
            aggregator
                .submit(Message::new_broker_message(
                    payload.clone(),
                    partition_0,
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
                    partition_1,
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
                partition,
                0,
                Utc::now(),
            ))
            .unwrap();
        aggregator
            .submit(Message::new_broker_message(
                make_payload(1_700_000_000, 1, 2, 3, &[(4, 3)]),
                partition,
                1,
                Utc::now(),
            ))
            .unwrap();
        // One minute later lands in a different bucket, plus has two category counts for the same message
        aggregator
            .submit(Message::new_broker_message(
                make_payload(1_700_000_060, 1, 2, 3, &[(4, 5), (7, 2)]),
                partition,
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
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
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
        // make sure new batch retains bucket_interval
        assert_eq!(aggregator.batch.bucket_interval, 60);
    }

    #[test]
    fn submit_returns_backpressure_when_message_carried_over() {
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
        struct RejectOnce {
            rejected: bool,
        }
        impl ProcessingStrategy<AggregatedOutcomesBatch> for RejectOnce {
            fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
            fn submit(
                &mut self,
                message: Message<AggregatedOutcomesBatch>,
            ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
                if !self.rejected {
                    self.rejected = true;
                    Err(SubmitError::MessageRejected(MessageRejected { message }))
                } else {
                    Ok(())
                }
            }
            fn terminate(&mut self) {}
            fn join(
                &mut self,
                _: Option<Duration>,
            ) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
        }

        let mut aggregator = OutcomesAggregator::new(
            RejectOnce { rejected: false },
            1, // flush after 1 bucket
            Duration::from_millis(30_000),
            60,
        );

        let partition = Partition::new(Topic::new("test"), 0);
        let payload = make_payload(6_000, 1, 2, 3, &[(4, 1)]);

        // First submit accumulates into batch
        aggregator
            .submit(Message::new_broker_message(
                payload.clone(),
                partition,
                0,
                Utc::now(),
            ))
            .unwrap();

        // poll triggers flush; next_step rejects → message_carried_over is set
        aggregator.poll().unwrap();
        assert!(aggregator.message_carried_over.is_some());

        // While carrying over, submit should return MessageRejected
        let result = aggregator.submit(Message::new_broker_message(
            payload.clone(),
            partition,
            1,
            Utc::now(),
        ));
        assert!(matches!(result, Err(SubmitError::MessageRejected(_))));

        // Next poll retries and succeeds; carried-over message clears
        aggregator.poll().unwrap();
        assert!(aggregator.message_carried_over.is_none());
    }

    #[test]
    fn join_honors_timeout_when_message_stays_carried_over() {
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
        struct AlwaysReject;
        impl ProcessingStrategy<AggregatedOutcomesBatch> for AlwaysReject {
            fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
            fn submit(
                &mut self,
                message: Message<AggregatedOutcomesBatch>,
            ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
                Err(SubmitError::MessageRejected(MessageRejected { message }))
            }
            fn terminate(&mut self) {}
            fn join(
                &mut self,
                _: Option<Duration>,
            ) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
        }

        let mut aggregator =
            OutcomesAggregator::new(AlwaysReject, 1, Duration::from_millis(30_000), 60);
        let partition = Partition::new(Topic::new("test"), 0);
        let payload = make_payload(6_000, 1, 2, 3, &[(4, 1)]);

        aggregator
            .submit(Message::new_broker_message(
                payload,
                partition,
                0,
                Utc::now(),
            ))
            .unwrap();

        let commit = aggregator.join(Some(Duration::from_millis(0))).unwrap();
        assert!(commit.is_none());
        assert!(aggregator.message_carried_over.is_some());
    }

    #[test]
    fn submit_uses_item_timestamp_when_enabled() {
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
        let _guard =
            override_options(&[("snuba", "consumer.use_item_timestamp", json!(true))]).unwrap();
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(2_000),
            60,
        );

        let topic = Topic::new("snuba-items");
        let partition = Partition::new(topic, 0);

        let trace_item = TraceItem {
            organization_id: 1,
            project_id: 2,
            received: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            timestamp: Some(Timestamp {
                seconds: 1_700_000_060,
                nanos: 0,
            }),
            outcomes: Some(Outcomes {
                key_id: 3,
                category_count: vec![CategoryCount {
                    data_category: 4,
                    quantity: 1,
                }],
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        trace_item.encode(&mut buf).unwrap();
        let payload = KafkaPayload::new(None, None, Some(buf));

        // we need to poll first in order to get the new value (true)
        aggregator.poll().unwrap();

        aggregator
            .submit(Message::new_broker_message(
                payload,
                partition,
                0,
                Utc::now(),
            ))
            .unwrap();

        let key = BucketKey {
            time_offset: 28_333_334, // 1_700_000_060 / 60
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        assert_eq!(
            aggregator.batch.buckets.get(&key).map(|s| s.quantity),
            Some(1)
        );
    }

    #[test]
    fn poll_updates_use_item_timestamp_dynamically() {
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(30_000),
            60,
        );

        let partition = Partition::new(Topic::new("snuba-items"), 0);

        let mut buf = Vec::new();
        TraceItem {
            organization_id: 1,
            project_id: 2,
            received: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            timestamp: Some(Timestamp {
                seconds: 1_700_000_060,
                nanos: 0,
            }),
            outcomes: Some(Outcomes {
                key_id: 3,
                category_count: vec![CategoryCount {
                    data_category: 4,
                    quantity: 1,
                }],
            }),
            ..Default::default()
        }
        .encode(&mut buf)
        .unwrap();
        let payload = KafkaPayload::new(None, None, Some(buf));

        let bucket_quantity = |aggregator: &OutcomesAggregator<Noop>, offset: u64| {
            let key = BucketKey {
                time_offset: offset,
                org_id: 1,
                project_id: 2,
                key_id: 3,
                category: 4,
            };
            aggregator.batch.buckets.get(&key).map(|s| s.quantity)
        };

        let mut offset = 0;
        let mut do_submit = |aggregator: &mut OutcomesAggregator<Noop>| {
            // Bypass the refresh throttle so the test sees option changes immediately.
            aggregator.last_options_refresh = None;
            aggregator.poll().unwrap();
            aggregator
                .submit(Message::new_broker_message(
                    payload.clone(),
                    partition,
                    offset,
                    Utc::now(),
                ))
                .unwrap();
            offset += 1;
        };

        // Enable item timestamp
        let guard =
            override_options(&[("snuba", "consumer.use_item_timestamp", json!(true))]).unwrap();
        do_submit(&mut aggregator);
        assert_eq!(bucket_quantity(&aggregator, 28_333_334), Some(1));
        assert_eq!(bucket_quantity(&aggregator, 28_333_333), None);

        // Disable item timestamp
        drop(guard);
        let _guard =
            override_options(&[("snuba", "consumer.use_item_timestamp", json!(false))]).unwrap();
        do_submit(&mut aggregator);
        assert_eq!(bucket_quantity(&aggregator, 28_333_333), Some(1));
        assert_eq!(bucket_quantity(&aggregator, 28_333_334), Some(1)); // still present from first submit
    }

    fn make_payload_with_item_id(
        ts_secs: i64,
        org_id: u64,
        project_id: u64,
        key_id: u64,
        item_id: [u8; 16],
        category_counts: &[(u32, u64)],
    ) -> KafkaPayload {
        let trace_item = TraceItem {
            organization_id: org_id,
            project_id,
            item_id: item_id.to_vec(),
            item_type: TraceItemType::Span.into(),
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
    fn submit_deduplicates_same_item_id() {
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(5_000),
            60,
        );

        let topic = Topic::new("snuba-items");
        let partition = Partition::new(topic, 0);
        let item_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        // Submit same item twice with quantity=5 each
        for offset in [0, 1] {
            aggregator
                .submit(Message::new_broker_message(
                    make_payload_with_item_id(1_700_000_000, 1, 2, 3, item_id, &[(4, 5)]),
                    partition,
                    offset,
                    Utc::now(),
                ))
                .unwrap();
        }

        let key = BucketKey {
            time_offset: 28_333_333,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        // Should only count once (quantity=5), not twice (quantity=10)
        assert_eq!(
            aggregator.batch.buckets.get(&key).map(|s| s.quantity),
            Some(5)
        );
    }

    #[test]
    fn submit_does_not_deduplicate_different_orgs() {
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(5_000),
            60,
        );

        let topic = Topic::new("snuba-items");
        let partition = Partition::new(topic, 0);
        let item_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        // Submit same item_id but for different orgs
        aggregator
            .submit(Message::new_broker_message(
                make_payload_with_item_id(1_700_000_000, 1, 2, 3, item_id, &[(4, 5)]),
                partition,
                0,
                Utc::now(),
            ))
            .unwrap();
        aggregator
            .submit(Message::new_broker_message(
                make_payload_with_item_id(1_700_000_000, 99, 2, 3, item_id, &[(4, 5)]),
                partition,
                1,
                Utc::now(),
            ))
            .unwrap();

        // Should have two separate buckets
        let key_org1 = BucketKey {
            time_offset: 28_333_333,
            org_id: 1,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        let key_org99 = BucketKey {
            time_offset: 28_333_333,
            org_id: 99,
            project_id: 2,
            key_id: 3,
            category: 4,
        };
        assert_eq!(
            aggregator.batch.buckets.get(&key_org1).map(|s| s.quantity),
            Some(5)
        );
        assert_eq!(
            aggregator.batch.buckets.get(&key_org99).map(|s| s.quantity),
            Some(5)
        );
    }

    #[test]
    fn is_duplicate_per_item_type() {
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(5_000),
            60,
        );

        let item_id: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let span_item = TraceItem {
            organization_id: 1,
            project_id: 2,
            item_id: item_id.clone(),
            item_type: TraceItemType::Span.into(),
            ..Default::default()
        };

        // First time: not a duplicate
        assert!(!aggregator.is_duplicate(&span_item));
        // Second time: duplicate, count incremented for this item type
        assert!(aggregator.is_duplicate(&span_item));
        assert_eq!(
            aggregator
                .batch
                .duplicate_item_count
                .get(&TraceItemType::Span),
            Some(&1)
        );
    }

    #[test]
    fn poll_throttles_option_refresh() {
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(30_000),
            60,
        );

        // First poll: option is true; cached value should pick it up.
        let guard =
            override_options(&[("snuba", "consumer.use_item_timestamp", json!(true))]).unwrap();
        aggregator.poll().unwrap();
        assert!(aggregator.use_item_timestamp);
        assert!(aggregator.last_options_refresh.is_some());

        // Flip the option and poll again immediately; throttle should keep cached value.
        drop(guard);
        let _guard =
            override_options(&[("snuba", "consumer.use_item_timestamp", json!(false))]).unwrap();
        aggregator.poll().unwrap();
        assert!(
            aggregator.use_item_timestamp,
            "throttle should keep stale cached value within OPTIONS_REFRESH_INTERVAL"
        );

        // Force the refresh window to be elapsed and poll again; cached value updates.
        aggregator.last_options_refresh = None;
        aggregator.poll().unwrap();
        assert!(!aggregator.use_item_timestamp);
    }

    #[test]
    fn is_duplicate_logs_when_option_enabled() {
        init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap();
        let _guard =
            override_options(&[("snuba", "consumer.log_duplicates", json!(true))]).unwrap();

        let mut aggregator = OutcomesAggregator::new(
            Noop { last_message: None },
            500,
            Duration::from_millis(30_000),
            60,
        );

        // Refresh options so log_duplicates picks up the override.
        aggregator.poll().unwrap();
        assert!(aggregator.log_duplicates);

        let item_id: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let span_item = TraceItem {
            organization_id: 1,
            project_id: 2,
            item_id: item_id.clone(),
            item_type: TraceItemType::Span.into(),
            ..Default::default()
        };

        // Counter behavior is unchanged regardless of log_duplicates.
        assert!(!aggregator.is_duplicate(&span_item));
        assert!(aggregator.is_duplicate(&span_item));
        assert_eq!(
            aggregator
                .batch
                .duplicate_item_count
                .get(&TraceItemType::Span),
            Some(&1)
        );
    }
}
