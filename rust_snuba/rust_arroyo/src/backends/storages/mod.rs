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

/// TODO: docs
pub trait MessageStorage<TPayload>: Send {
    /// Create a topic with the given number of partitions.
    ///
    /// # Errors
    /// If the topic already exists, [`TopicExists`] will be returned.
    fn create_topic(&mut self, topic: Topic, partitions: u16) -> Result<(), TopicExists>;

    /// List all topics.
    fn list_topics(&self) -> Vec<&Topic>;

    /// Delete a topic.
    ///
    /// # Errors
    /// If the topic does not exist, [`TopicDoesNotExist`] will be returned.
    fn delete_topic(&mut self, topic: &Topic) -> Result<(), TopicDoesNotExist>;

    /// Get the number of partitions within a topic.
    ///
    /// # Errors
    /// If the topic does not exist, [`TopicDoesNotExist`] will be returned.
    fn partition_count(&self, topic: &Topic) -> Result<u16, TopicDoesNotExist>;

    /// Consume a message from the provided partition, reading from the given
    /// offset. If no message exists at the given offset when reading from
    /// the tail of the partition, this method returns `Ok(None)`.
    ///
    /// # Errors
    /// * If the offset is out of range (there are no messages, and we're not
    /// reading from the tail of the partition where the next message would
    /// be if it existed), [`OffsetOutOfRange`] will be returned.
    ///
    /// * If the topic does not exist, [`TopicDoesNotExist`] will
    /// be returned.
    ///
    /// * If the topic exists but the partition does not,
    /// [`PartitionDoesNotExist`] will be returned.
    fn consume(
        &self,
        partition: &Partition,
        offset: u64,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumeError>;

    /// Produce a single message to the provided partition.
    ///
    /// # Errors
    /// * If the topic does not exist, [`TopicDoesNotExist`] will
    /// be returned.
    ///
    /// * If the topic exists but the partition does not,
    /// [`PartitionDoesNotExist`] will be returned.
    fn produce(
        &mut self,
        partition: &Partition,
        payload: TPayload,
        timestamp: DateTime<Utc>,
    ) -> Result<u64, ConsumeError>;
}
