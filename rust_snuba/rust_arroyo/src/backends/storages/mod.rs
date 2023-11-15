pub mod memory;
use super::super::types::{BrokerMessage, Partition, Topic};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct TopicExists;

#[derive(Debug, Clone)]
pub struct TopicDoesNotExist;

#[derive(Debug, Clone)]
pub struct PartitionDoesNotExist;

#[derive(Debug, Clone)]
pub struct OffsetOutOfRange;

#[derive(Debug)]
pub enum ConsumeError {
    TopicDoesNotExist,
    PartitionDoesNotExist,
    OffsetOutOfRange,
}

pub trait MessageStorage<TPayload: Clone>: Send {
    // Create a topic with the given number of partitions.
    //
    // If the topic already exists, a ``TopicExists`` exception will be
    // raised.
    fn create_topic(&mut self, topic: Topic, partitions: u16) -> Result<(), TopicExists>;

    // List all topics.
    fn list_topics(&self) -> Vec<&Topic>;

    // Delete a topic.
    //
    // If the topic does not exist, a ``TopicDoesNotExist`` exception will
    // be raised.
    fn delete_topic(&mut self, topic: &Topic) -> Result<(), TopicDoesNotExist>;

    // Get the number of partitions within a topic.
    //
    // If the topic does not exist, a ``TopicDoesNotExist`` exception will
    // be raised.
    fn get_partition_count(&self, topic: &Topic) -> Result<u16, TopicDoesNotExist>;

    fn get_partition(&self, topic: &Topic, index: u16) -> Result<Partition, ConsumeError>;

    // Consume a message from the provided partition, reading from the given
    // offset. If no message exists at the given offset when reading from
    // the tail of the partition, this method returns ``None``.
    //
    // If the offset is out of range (there are no messages, and we're not
    // reading from the tail of the partition where the next message would
    // be if it existed), an ``OffsetOutOfRange`` exception will be raised.
    //
    // If the topic does not exist, a ``TopicDoesNotExist`` exception will
    // be raised. If the topic exists but the partition does not, a
    // ``PartitionDoesNotExist`` exception will be raised.
    fn consume(
        &self,
        partition: &Partition,
        offset: u64,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumeError>;

    // Produce a single message to the provided partition.
    //
    // If the topic does not exist, a ``TopicDoesNotExist`` exception will
    // be raised. If the topic exists but the partition does not, a
    // ``PartitionDoesNotExist`` exception will be raised.
    fn produce(
        &mut self,
        partition: &Partition,
        payload: TPayload,
        timestamp: DateTime<Utc>,
    ) -> Result<u64, ConsumeError>;
}
