use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use crate::types::{BrokerMessage, Partition, Topic, TopicOrPartition};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub trait DlqProducer<TPayload>: Send + Sync {
    // Send a message to the DLQ.
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>>;

    fn build_initial_state(
        &self,
        limit: DlqLimit,
        assignment: &HashMap<Partition, u64>,
    ) -> DlqLimitState;
}

// Drops all invalid messages. Produce returns an immediately resolved future.
struct NoopDlqProducer {}

impl<TPayload: Send + Sync + 'static> DlqProducer<TPayload> for NoopDlqProducer {
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>> {
        Box::pin(async move { message })
    }

    fn build_initial_state(
        &self,
        limit: DlqLimit,
        _assignment: &HashMap<Partition, u64>,
    ) -> DlqLimitState {
        DlqLimitState::new(limit, HashMap::new())
    }
}

// KafkaDlqProducer forwards invalid messages to a Kafka topic

// Two additional fields are added to the headers of the Kafka message
// "original_partition": The partition of the original message
// "original_offset": The offset of the original message
pub struct KafkaDlqProducer {
    producer: Arc<KafkaProducer>,
    topic: TopicOrPartition,
}

impl KafkaDlqProducer {
    pub fn new(producer: KafkaProducer, topic: Topic) -> Self {
        Self {
            producer: Arc::new(producer),
            topic: TopicOrPartition::Topic(topic),
        }
    }
}

impl DlqProducer<KafkaPayload> for KafkaDlqProducer {
    fn produce(
        &self,
        message: BrokerMessage<KafkaPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<KafkaPayload>> + Send + Sync>> {
        let producer = self.producer.clone();
        let topic = self.topic;

        let mut headers = message.payload.headers().cloned().unwrap_or_default();

        headers = headers.insert(
            "original_partition",
            Some(message.offset.to_string().into_bytes()),
        );
        headers = headers.insert(
            "original_offset",
            Some(message.offset.to_string().into_bytes()),
        );

        let payload = KafkaPayload::new(
            message.payload.key().cloned(),
            Some(headers),
            message.payload.payload().cloned(),
        );

        Box::pin(async move {
            producer
                .produce(&topic, payload)
                .expect("Message was produced");

            message
        })
    }

    fn build_initial_state(
        &self,
        limit: DlqLimit,
        assignment: &HashMap<Partition, u64>,
    ) -> DlqLimitState {
        // XXX: We assume the last offsets were invalid when starting the consumer
        DlqLimitState::new(
            limit,
            assignment
                .iter()
                .filter_map(|(p, o)| {
                    Some((*p, o.checked_sub(1).map(InvalidMessageStats::invalid_at)?))
                })
                .collect(),
        )
    }
}

/// Defines any limits that should be placed on the number of messages that are
/// forwarded to the DLQ. This exists to prevent 100% of messages from going into
/// the DLQ if something is misconfigured or bad code is deployed. In this scenario,
/// it may be preferable to stop processing messages altogether and deploy a fix
/// rather than rerouting every message to the DLQ.
///
/// The ratio and max_consecutive_count are counted on a per-partition basis.
///
/// The default is no limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct DlqLimit {
    pub max_invalid_ratio: Option<f64>,
    pub max_consecutive_count: Option<u64>,
}

/// A record of valid and invalid messages that have been received on a partition.
#[derive(Debug, Clone, Copy, Default)]
pub struct InvalidMessageStats {
    /// The number of valid messages that have been received.
    pub valid: u64,
    /// The number of invalid messages that have been received.
    pub invalid: u64,
    /// The length of the current run of received invalid messages.
    pub consecutive_invalid: u64,
    /// The offset of the last received invalid message.
    pub last_invalid_offset: u64,
}

impl InvalidMessageStats {
    /// Creates an empty record with the last invalid message received at `offset`.
    ///
    /// The `invalid` and `consecutive_invalid` fields are intentionally left at 0.
    pub fn invalid_at(offset: u64) -> Self {
        Self {
            last_invalid_offset: offset,
            ..Default::default()
        }
    }
}

/// Struct that keeps a record of how many valid and invalid messages have been received
/// per partition and decides whether to produce a message to the DLQ according to a configured limit.
#[derive(Debug, Clone, Default)]
pub struct DlqLimitState {
    limit: DlqLimit,
    records: HashMap<Partition, InvalidMessageStats>,
}

impl DlqLimitState {
    /// Creates a `DlqLimitState` with a given limit and initial set of records.
    pub fn new(limit: DlqLimit, records: HashMap<Partition, InvalidMessageStats>) -> Self {
        Self { limit, records }
    }

    /// Records an invalid message.
    ///
    /// This updates the internal statistics about the message's partition and
    /// returns `true` if the message should be produced to the DLQ according to the
    /// configured limit.
    fn record_invalid_message<T>(&mut self, message: &BrokerMessage<T>) -> bool {
        let record = self
            .records
            .entry(message.partition)
            .and_modify(|record| {
                let last_invalid = record.last_invalid_offset;
                match message.offset {
                    o if o <= last_invalid => {
                        tracing::error!("Invalid message raised out of order")
                    }
                    o if o == last_invalid + 1 => record.consecutive_invalid += 1,
                    o => {
                        let valid_count = o - last_invalid + 1;
                        record.valid += valid_count;
                        record.consecutive_invalid = 1;
                    }
                }

                record.invalid += 1;
                record.last_invalid_offset = message.offset;
            })
            .or_insert(InvalidMessageStats {
                valid: 0,
                invalid: 1,
                consecutive_invalid: 1,
                last_invalid_offset: message.offset,
            });

        if let Some(max_invalid_ratio) = self.limit.max_invalid_ratio {
            if record.valid == 0 {
                // When no valid messages have been processed, we should not
                // accept the message into the dlq. It could be an indicator
                // of severe problems on the pipeline. It is best to let the
                // consumer backlog in those cases.
                return false;
            }

            if (record.invalid as f64) / (record.valid as f64) > max_invalid_ratio {
                return false;
            }
        }

        if let Some(max_consecutive_count) = self.limit.max_consecutive_count {
            if record.consecutive_invalid > max_consecutive_count {
                return false;
            }
        }
        true
    }
}

/// DLQ policy defines the DLQ configuration, and is passed to the stream processor
/// upon creation of the consumer. It consists of the DLQ producer implementation and
/// any limits that should be applied.
pub struct DlqPolicy<TPayload> {
    producer: Box<dyn DlqProducer<TPayload>>,
    limit: DlqLimit,
    max_buffered_messages_per_partition: Option<usize>,
}

impl<TPayload> DlqPolicy<TPayload> {
    pub fn new(
        producer: Box<dyn DlqProducer<TPayload>>,
        limit: DlqLimit,
        max_buffered_messages_per_partition: Option<usize>,
    ) -> Self {
        DlqPolicy {
            producer,
            limit,
            max_buffered_messages_per_partition,
        }
    }
}

impl<TPayload> fmt::Debug for DlqPolicy<TPayload> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DlqPolicy")
            .field("limit", &self.limit)
            .field(
                "max_buffered_messages_per_partition",
                &self.max_buffered_messages_per_partition,
            )
            .finish_non_exhaustive()
    }
}

// Wraps the DLQ policy and keeps track of messages pending produce/commit.
type Futures<TPayload> = VecDeque<(u64, JoinHandle<BrokerMessage<TPayload>>)>;

pub(crate) struct DlqPolicyWrapper<TPayload> {
    dlq_policy: Option<DlqPolicy<TPayload>>,
    dlq_limit_state: DlqLimitState,
    // need to keep concurrency config around in order to not drop runtime
    concurrency_config: ConcurrencyConfig,
    // This is a per-partition max
    max_pending_futures: usize,
    futures: BTreeMap<Partition, Futures<TPayload>>,
}

impl<TPayload: Send + Sync + 'static> DlqPolicyWrapper<TPayload> {
    pub fn new(dlq_policy: Option<DlqPolicy<TPayload>>) -> Self {
        let concurrency_config = ConcurrencyConfig::new(10);
        DlqPolicyWrapper {
            dlq_policy,
            dlq_limit_state: DlqLimitState::default(),
            concurrency_config,
            max_pending_futures: 1000,
            futures: BTreeMap::new(),
        }
    }

    pub fn max_buffered_messages_per_partition(&self) -> Option<usize> {
        self.dlq_policy
            .as_ref()
            .and_then(|p| p.max_buffered_messages_per_partition)
    }

    /// Clears the DLQ limits.
    pub fn reset_dlq_limits(&mut self, assignment: &HashMap<Partition, u64>) {
        let Some(policy) = self.dlq_policy.as_ref() else {
            return;
        };

        self.dlq_limit_state = policy
            .producer
            .build_initial_state(policy.limit, assignment);
    }

    // Removes all completed futures, then appends a future with message to be produced
    // to the queue. Blocks if there are too many pending futures until some are done.
    pub fn produce(&mut self, message: BrokerMessage<TPayload>) {
        for (_p, values) in self.futures.iter_mut() {
            while !values.is_empty() {
                let len = values.len();
                let (_, future) = &mut values[0];
                if future.is_finished() || len >= self.max_pending_futures {
                    let res = self.concurrency_config.handle().block_on(future);
                    if let Err(err) = res {
                        tracing::error!("Error producing to DLQ: {}", err);
                    }
                    values.pop_front();
                } else {
                    break;
                }
            }
        }

        if let Some(dlq_policy) = &self.dlq_policy {
            if self.dlq_limit_state.record_invalid_message(&message) {
                tracing::info!("producing message to dlq");
                let (partition, offset) = (message.partition, message.offset);

                let task = dlq_policy.producer.produce(message);
                let handle = self.concurrency_config.handle().spawn(task);

                self.futures
                    .entry(partition)
                    .or_default()
                    .push_back((offset, handle));
            } else {
                panic!("DLQ limit was reached");
            }
        } else {
            tracing::info!("dlq policy missing, dropping message");
        }
    }

    // Blocks until all messages up to the committable have been produced so
    // they are safe to commit.
    pub fn flush(&mut self, committable: &HashMap<Partition, u64>) {
        for (&p, &committable_offset) in committable {
            if let Some(values) = self.futures.get_mut(&p) {
                while let Some((offset, future)) = values.front_mut() {
                    // The committable offset is message's offset + 1
                    if committable_offset > *offset {
                        if let Err(error) = self.concurrency_config.handle().block_on(future) {
                            let error: &dyn std::error::Error = &error;
                            tracing::error!(error, "Error producing to DLQ");
                        }

                        values.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

/// Stores messages that are pending commit. This is used to retreive raw messages
// in case they need to be placed in the DLQ.
#[derive(Debug, Clone, Default)]
pub struct BufferedMessages<TPayload> {
    max_per_partition: Option<usize>,
    buffered_messages: BTreeMap<Partition, VecDeque<BrokerMessage<TPayload>>>,
}

impl<TPayload> BufferedMessages<TPayload> {
    pub fn new(max_per_partition: Option<usize>) -> Self {
        BufferedMessages {
            max_per_partition,
            buffered_messages: BTreeMap::new(),
        }
    }

    // Add message to the buffer.
    pub fn append(&mut self, message: BrokerMessage<TPayload>) {
        let buffered = self.buffered_messages.entry(message.partition).or_default();
        if let Some(max) = self.max_per_partition {
            if max == 0 {
                return;
            }

            if buffered.len() >= max {
                tracing::warn!(
                    "DLQ buffer exceeded, dropping message on partition {}",
                    message.partition.index
                );
                buffered.pop_front();
            }
        }

        buffered.push_back(message);
    }

    /// Return the message at the given offset or None if it is not found in the buffer.
    /// Messages up to the offset for the given partition are removed.
    pub fn pop(&mut self, partition: &Partition, offset: u64) -> Option<BrokerMessage<TPayload>> {
        let messages = self.buffered_messages.get_mut(partition)?;
        while let Some(message) = messages.front() {
            match message.offset.cmp(&offset) {
                Ordering::Equal => {
                    return messages.pop_front();
                }
                Ordering::Greater => {
                    return None;
                }
                Ordering::Less => {
                    messages.pop_front();
                }
            };
        }

        None
    }

    // Clear the buffer. Should be called on rebalance.
    pub fn reset(&mut self) {
        self.buffered_messages.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Topic;
    use chrono::Utc;
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn test_buffered_messages() {
        let mut buffer = BufferedMessages::new(None);
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        assert_eq!(buffer.pop(&partition, 0).unwrap().offset, 0);
        assert_eq!(buffer.pop(&partition, 8).unwrap().offset, 8);
        assert!(buffer.pop(&partition, 1).is_none()); // Removed when we popped offset 8
        assert_eq!(buffer.pop(&partition, 9).unwrap().offset, 9);
        assert!(buffer.pop(&partition, 10).is_none()); // Doesn't exist
    }

    #[test]
    fn test_buffered_messages_limit() {
        let mut buffer = BufferedMessages::new(Some(2));
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        // It's gone
        assert!(buffer.pop(&partition, 1).is_none());

        assert_eq!(buffer.pop(&partition, 9).unwrap().payload, 9);
    }

    #[test]
    fn test_no_buffered_messages() {
        let mut buffer = BufferedMessages::new(Some(0));
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        assert!(buffer.pop(&partition, 9).is_none());
    }

    #[derive(Clone)]
    struct TestDlqProducer {
        pub call_count: Arc<Mutex<usize>>,
    }

    impl TestDlqProducer {
        fn new() -> Self {
            TestDlqProducer {
                call_count: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl<TPayload: Send + Sync + 'static> DlqProducer<TPayload> for TestDlqProducer {
        fn produce(
            &self,
            message: BrokerMessage<TPayload>,
        ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>> {
            *self.call_count.lock().unwrap() += 1;
            Box::pin(async move { message })
        }

        fn build_initial_state(
            &self,
            limit: DlqLimit,
            assignment: &HashMap<Partition, u64>,
        ) -> DlqLimitState {
            DlqLimitState::new(
                limit,
                assignment
                    .iter()
                    .map(|(p, _)| (*p, InvalidMessageStats::default()))
                    .collect(),
            )
        }
    }

    #[test]
    fn test_dlq_policy_wrapper() {
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        let producer = TestDlqProducer::new();

        let mut wrapper = DlqPolicyWrapper::new(Some(DlqPolicy::new(
            Box::new(producer.clone()),
            DlqLimit::default(),
            None,
        )));

        wrapper.reset_dlq_limits(&HashMap::from([(partition, 0)]));

        for i in 0..10 {
            wrapper.produce(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        wrapper.flush(&HashMap::from([(partition, 11)]));

        assert_eq!(*producer.call_count.lock().unwrap(), 10);
    }

    #[test]
    #[should_panic]
    fn test_dlq_policy_wrapper_limit_exceeded() {
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        let producer = TestDlqProducer::new();

        let mut wrapper = DlqPolicyWrapper::new(Some(DlqPolicy::new(
            Box::new(producer),
            DlqLimit {
                max_consecutive_count: Some(5),
                ..Default::default()
            },
            None,
        )));

        wrapper.reset_dlq_limits(&HashMap::from([(partition, 0)]));

        for i in 0..10 {
            wrapper.produce(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        wrapper.flush(&HashMap::from([(partition, 11)]));
    }

    #[test]
    fn test_dlq_limit_state() {
        let partition = Partition::new(Topic::new("test_topic"), 0);
        let limit = DlqLimit {
            max_consecutive_count: Some(5),
            ..Default::default()
        };

        let mut state = DlqLimitState::new(
            limit,
            HashMap::from([(partition, InvalidMessageStats::invalid_at(3))]),
        );

        // 1 valid message followed by 4 invalid
        for i in 4..9 {
            let msg = BrokerMessage::new(i, partition, i, chrono::Utc::now());
            assert!(state.record_invalid_message(&msg));
        }

        // Next message should not be accepted
        let msg = BrokerMessage::new(9, partition, 9, chrono::Utc::now());
        assert!(!state.record_invalid_message(&msg));
    }
}
