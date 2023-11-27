use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use crate::types::{BrokerMessage, Partition, Topic, TopicOrPartition};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

pub trait DlqProducer<TPayload> {
    // Send a message to the DLQ.
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>>;

    fn build_initial_state(&self) -> DlqLimitState;
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

    fn build_initial_state(&self) -> DlqLimitState {
        DlqLimitState {}
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

    fn build_initial_state(&self) -> DlqLimitState {
        DlqLimitState {}
    }
}

// Defines any limits that should be placed on the number of messages that are
// forwarded to the DLQ. This exists to prevent 100% of messages from going into
// the DLQ if something is misconfigured or bad code is deployed. In this scenario,
// it may be preferable to stop processing messages altogether and deploy a fix
// rather than rerouting every message to the DLQ.

// The ratio and max_consecutive_count are counted on a per-partition basis.
pub struct DlqLimit {
    pub max_invalid_ratio: Option<f64>,
    pub max_consecutive_count: Option<u64>,
}

pub struct DlqLimitState {}

// DLQ policy defines the DLQ configuration, and is passed to the stream processor
// upon creation of the consumer. It consists of the DLQ producer implementation and
// any limits that should be applied.
//
// TODO: Respect DLQ limits
pub struct DlqPolicy<TPayload> {
    producer: Box<dyn DlqProducer<TPayload>>,
    #[allow(dead_code)]
    limit: DlqLimit,
}

impl<TPayload> DlqPolicy<TPayload> {
    pub fn new(producer: Box<dyn DlqProducer<TPayload>>, limit: DlqLimit) -> Self {
        DlqPolicy { producer, limit }
    }
}

// Wraps the DLQ policy and keeps track of messages pending produce/commit.
type Futures<TPayload> = VecDeque<(u64, JoinHandle<BrokerMessage<TPayload>>)>;

pub(crate) struct DlqPolicyWrapper<TPayload> {
    dlq_policy: Option<DlqPolicy<TPayload>>,
    runtime: Handle,
    // This is a per-partition max
    max_pending_futures: usize,
    futures: BTreeMap<Partition, Futures<TPayload>>,
}

impl<TPayload: Clone + Send + Sync + 'static> DlqPolicyWrapper<TPayload> {
    pub fn new(dlq_policy: Option<DlqPolicy<TPayload>>) -> Self {
        let concurrency_config = ConcurrencyConfig::new(10);
        DlqPolicyWrapper {
            dlq_policy,
            runtime: concurrency_config.handle(),
            max_pending_futures: 1000,
            futures: BTreeMap::new(),
        }
    }

    // Removes all completed futures, then appends a future with message to be produced
    // to the queue. Blocks if there are too many pending futures until some are done.
    pub fn produce(&mut self, message: BrokerMessage<TPayload>) {
        for (_p, values) in self.futures.iter_mut() {
            while !values.is_empty() {
                let len = values.len();
                let (_, future) = &mut values[0];
                if future.is_finished() {
                    values.pop_front();
                } else if len >= self.max_pending_futures {
                    let res = self.runtime.block_on(future);
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
            let task = dlq_policy.producer.produce(message.clone());
            let handle = self.runtime.spawn(task);

            self.futures
                .entry(message.partition)
                .or_default()
                .push_back((message.offset, handle));
        }
    }

    // Blocks until all messages up to the committable have been produced so
    // they are safe to commit.
    pub fn flush(&mut self, committable: HashMap<Partition, u64>) {
        for (p, values) in self.futures.iter_mut() {
            while !values.is_empty() {
                let (offset, future) = &mut values[0];
                if let Some(committable_offset) = committable.get(p) {
                    // The committable offset is message's offset + 1
                    if *committable_offset > *offset {
                        let res: Result<BrokerMessage<TPayload>, tokio::task::JoinError> =
                            self.runtime.block_on(future);

                        if let Err(err) = res {
                            tracing::error!("Error producing to DLQ: {}", err);
                        } else {
                            values.pop_front();
                        }
                    }
                }
            }
        }
    }
}

/// Stores messages that are pending commit. This is used to retreive raw messages
// in case they need to be placed in the DLQ.
pub struct BufferedMessages<TPayload> {
    buffered_messages: BTreeMap<Partition, VecDeque<BrokerMessage<TPayload>>>,
}

impl<TPayload> Default for BufferedMessages<TPayload> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TPayload> BufferedMessages<TPayload> {
    pub fn new() -> Self {
        BufferedMessages {
            buffered_messages: BTreeMap::new(),
        }
    }

    // Add message to the buffer.
    pub fn append(&mut self, message: BrokerMessage<TPayload>) {
        if let Some(messages) = self.buffered_messages.get_mut(&message.partition) {
            messages.push_back(message);
        } else {
            self.buffered_messages
                .insert(message.partition, VecDeque::from([message]));
        };
    }

    // Return the message at the given offset or None if it is not found in the buffer.
    // Messages up to the offset for the given partition are removed.
    pub fn pop(&mut self, partition: &Partition, offset: u64) -> Option<BrokerMessage<TPayload>> {
        if let Some(messages) = self.buffered_messages.get_mut(partition) {
            #[allow(clippy::never_loop)]
            while let Some(message) = messages.front_mut() {
                match message.offset.cmp(&offset) {
                    Ordering::Equal => {
                        return messages.pop_front();
                    }
                    Ordering::Greater => {
                        break;
                    }
                    Ordering::Less => {
                        messages.pop_front();
                    }
                };
            }
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
        let mut buffer = BufferedMessages::new();
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
        assert_eq!(buffer.pop(&partition, 1), None); // Removed when we popped offset 8
        assert_eq!(buffer.pop(&partition, 9).unwrap().offset, 9);
        assert_eq!(buffer.pop(&partition, 10), None); // Doesn't exist
    }

    #[test]
    fn test_dlq_policy_wrapper() {
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

            fn build_initial_state(&self) -> DlqLimitState {
                DlqLimitState {}
            }
        }

        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        let producer = TestDlqProducer::new();

        let mut wrapper = DlqPolicyWrapper::new(Some(DlqPolicy::new(
            Box::new(producer.clone()),
            DlqLimit {
                max_invalid_ratio: None,
                max_consecutive_count: Some(1),
            },
        )));

        for i in 0..10 {
            wrapper.produce(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        wrapper.flush(HashMap::from([(partition, 11)]));

        assert_eq!(*producer.call_count.lock().unwrap(), 10);
    }
}
