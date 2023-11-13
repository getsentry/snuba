use super::{ConsumeError, MessageStorage, TopicDoesNotExist, TopicExists};
use crate::types::{BrokerMessage, Partition, Topic};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;

/// Stores a list of messages for each partition of a topic.
///
/// `self.messages[i][j]` is the `j`-th message of the `i`-th partition.
struct TopicMessages<TPayload> {
    messages: Vec<Vec<BrokerMessage<TPayload>>>,
}

impl<TPayload> TopicMessages<TPayload> {
    /// Creates empty messsage queues for the given number of partitions.
    fn new(partitions: u16) -> Self {
        Self {
            messages: (0..partitions).map(|_| Vec::new()).collect(),
        }
    }

    /// Returns the messages of the given partition.
    ///
    /// # Errors
    /// Returns `ConsumeError::PartitionDoesNotExist` if the partition number is out of bounds.
    fn get_messages(&self, partition: u16) -> Result<&Vec<BrokerMessage<TPayload>>, ConsumeError> {
        self.messages
            .get(partition as usize)
            .ok_or(ConsumeError::PartitionDoesNotExist)
    }

    /// Appends the given message to its partition's message queue.
    ///
    /// # Errors
    /// Returns `ConsumeError::PartitionDoesNotExist` if the message's partition number is out of bounds.
    fn add_message(&mut self, message: BrokerMessage<TPayload>) -> Result<(), ConsumeError> {
        let stream = self
            .messages
            .get_mut(message.partition.index as usize)
            .ok_or(ConsumeError::PartitionDoesNotExist)?;
        stream.push(message);
        Ok(())
    }

    /// Returns the number of partitions.
    fn partition_count(&self) -> u16 {
        u16::try_from(self.messages.len()).unwrap()
    }
}

pub struct MemoryMessageStorage<TPayload: Clone> {
    topics: HashMap<Topic, TopicMessages<TPayload>>,
}

impl<TPayload: Clone> Default for MemoryMessageStorage<TPayload> {
    fn default() -> Self {
        MemoryMessageStorage {
            topics: HashMap::new(),
        }
    }
}

impl<TPayload: Clone + Send> MessageStorage<TPayload> for MemoryMessageStorage<TPayload> {
    fn create_topic(&mut self, topic: Topic, partitions: u16) -> Result<(), TopicExists> {
        if self.topics.contains_key(&topic) {
            return Err(TopicExists);
        }
        self.topics
            .insert(topic.clone(), TopicMessages::new(partitions));
        Ok(())
    }

    fn list_topics(&self) -> Vec<&Topic> {
        let it = self.topics.keys();
        let mut ret: Vec<&Topic> = Vec::new();
        for x in it {
            ret.push(x);
        }
        ret
    }

    fn delete_topic(&mut self, topic: &Topic) -> Result<(), TopicDoesNotExist> {
        if !self.topics.contains_key(topic) {
            return Err(TopicDoesNotExist);
        }
        self.topics.remove(topic);
        Ok(())
    }

    fn get_partition_count(&self, topic: &Topic) -> Result<u16, TopicDoesNotExist> {
        match self.topics.get(topic) {
            Some(x) => Ok(x.partition_count()),
            None => Err(TopicDoesNotExist),
        }
    }

    fn get_partition(&self, topic: &Topic, index: u16) -> Result<Partition, ConsumeError> {
        let content = self
            .topics
            .get(topic)
            .ok_or(ConsumeError::TopicDoesNotExist)?;
        if content.partition_count() > index {
            Ok(Partition {
                topic: topic.clone(),
                index,
            })
        } else {
            Err(ConsumeError::PartitionDoesNotExist)
        }
    }

    fn consume(
        &self,
        partition: &Partition,
        offset: u64,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumeError> {
        let n_offset = usize::try_from(offset).unwrap();
        let messages = self.topics[&partition.topic].get_messages(partition.index)?;
        match messages.len().cmp(&n_offset) {
            Ordering::Greater => Ok(Some(messages[n_offset].clone())),
            Ordering::Less => Err(ConsumeError::OffsetOutOfRange),
            Ordering::Equal => Ok(None),
        }
    }

    fn produce(
        &mut self,
        partition: &Partition,
        payload: TPayload,
        timestamp: DateTime<Utc>,
    ) -> Result<u64, ConsumeError> {
        let messages = self
            .topics
            .get_mut(&partition.topic)
            .ok_or(ConsumeError::TopicDoesNotExist)?;
        let offset = messages.get_messages(partition.index)?.len();
        let _ = messages.add_message(BrokerMessage::new(
            payload,
            partition.clone(),
            u64::try_from(offset).unwrap(),
            timestamp,
        ));
        Ok(u64::try_from(offset).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryMessageStorage;
    use super::TopicMessages;
    use crate::backends::storages::MessageStorage;
    use crate::types::{BrokerMessage, Partition, Topic};
    use chrono::Utc;

    #[test]
    fn test_partition_count() {
        let topic: TopicMessages<String> = TopicMessages::new(64);
        assert_eq!(topic.partition_count(), 64);
    }

    #[test]
    fn test_empty_partitions() {
        let topic: TopicMessages<String> = TopicMessages::new(2);
        assert_eq!(topic.get_messages(0).unwrap().len(), 0);
        assert_eq!(topic.get_messages(1).unwrap().len(), 0);
    }

    #[test]
    fn test_invalid_partition() {
        let topic: TopicMessages<String> = TopicMessages::new(2);
        assert!(topic.get_messages(10).is_err());
    }

    #[test]
    fn test_add_messages() {
        let mut topic: TopicMessages<String> = TopicMessages::new(2);
        let now = Utc::now();
        let p = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };
        let res = topic.add_message(BrokerMessage::new("payload".to_string(), p, 10, now));

        assert!(res.is_ok());
        assert_eq!(topic.get_messages(0).unwrap().len(), 1);

        let queue = topic.get_messages(0).unwrap();
        assert_eq!(queue[0].offset, 10);

        assert_eq!(topic.get_messages(1).unwrap().len(), 0);
    }

    #[test]
    fn create_manage_topic() {
        let mut m: MemoryMessageStorage<String> = Default::default();
        let res = m.create_topic(
            Topic {
                name: "test".to_string(),
            },
            16,
        );
        assert!(res.is_ok());
        let b = m.list_topics();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].name, "test".to_string());

        let t = Topic {
            name: "test".to_string(),
        };
        let res2 = m.delete_topic(&t);
        assert!(res2.is_ok());
        let b2 = m.list_topics();
        assert_eq!(b2.len(), 0);
    }

    #[test]
    fn test_mem_partition_count() {
        let mut m: MemoryMessageStorage<String> = Default::default();
        let _ = m.create_topic(
            Topic {
                name: "test".to_string(),
            },
            16,
        );

        assert_eq!(
            m.get_partition_count(&Topic {
                name: "test".to_string()
            })
            .unwrap(),
            16
        );
    }

    #[test]
    fn test_consume_empty() {
        let mut m: MemoryMessageStorage<String> = Default::default();
        let _ = m.create_topic(
            Topic {
                name: "test".to_string(),
            },
            16,
        );
        let p = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };
        let message = m.consume(&p, 0);
        assert!(message.is_ok());
        assert!(message.unwrap().is_none());

        let err = m.consume(&p, 1);
        assert!(err.is_err());
    }

    #[test]
    fn test_produce() {
        let mut m: MemoryMessageStorage<String> = Default::default();
        let _ = m.create_topic(
            Topic {
                name: "test".to_string(),
            },
            2,
        );
        let p = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };
        let time = Utc::now();
        let offset = m.produce(&p, "test".to_string(), time).unwrap();
        assert_eq!(offset, 0);

        let msg_c = m.consume(&p, 0).unwrap();
        assert!(msg_c.is_some());
        let existing_msg = msg_c.unwrap();
        assert_eq!(existing_msg.offset, 0);
        assert_eq!(existing_msg.payload, "test".to_string());

        let msg_none = m.consume(&p, 1).unwrap();
        assert!(msg_none.is_none());
    }
}
