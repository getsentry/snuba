#![allow(dead_code)]
use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::types::{BrokerMessage, Partition, Topic, TopicOrPartition};
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub trait DlqProducer<TPayload> {
    // Send a message to the DLQ.
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>>>>;

    fn build_initial_state(&self) -> DlqLimitState;
}

// Drops all invalid messages. Produce returns an immediately resolved future.
struct NoopDlqProducer {}

impl<TPayload: 'static> DlqProducer<TPayload> for NoopDlqProducer {
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>>>> {
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
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<KafkaPayload>>>> {
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
pub struct DlqPolicy<TPayload> {
    producer: Box<dyn DlqProducer<TPayload>>,
    limit: DlqLimit,
}

impl<TPayload> DlqPolicy<TPayload> {
    pub fn new(producer: Box<dyn DlqProducer<TPayload>>, limit: DlqLimit) -> Self {
        DlqPolicy { producer, limit }
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
}
