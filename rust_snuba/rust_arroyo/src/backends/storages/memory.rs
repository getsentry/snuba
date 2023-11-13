use super::{ConsumeError, MessageStorage, TopicDoesNotExist, TopicExists};
use crate::types::{BrokerMessage, Partition, Topic};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;

struct TopicContent<TPayload> {
    partition_meta: Vec<Partition>,
    partitions: HashMap<Partition, Vec<BrokerMessage<TPayload>>>,
}

impl<TPayload> TopicContent<TPayload> {
    pub fn new(topic: &Topic, partitions: u16) -> Self {
        let mut queues = HashMap::new();
        let mut part_meta = Vec::new();
        for i in 0..partitions {
            let p = Partition {
                topic: topic.clone(),
                index: i,
            };
            part_meta.push(p.clone());
            queues.insert(p.clone(), Vec::new());
        }
        Self {
            partitions: queues,
            partition_meta: part_meta,
        }
    }

    fn get_messages(
        &self,
        partition: &Partition,
    ) -> Result<&Vec<BrokerMessage<TPayload>>, ConsumeError> {
        if !self.partition_meta.contains(partition) {
            return Err(ConsumeError::PartitionDoesNotExist);
        }
        Ok(&self.partitions[partition])
    }

    fn add_message(&mut self, message: BrokerMessage<TPayload>) -> Result<(), ConsumeError> {
        if !self.partition_meta.contains(&message.partition) {
            return Err(ConsumeError::PartitionDoesNotExist);
        }
        let stream = self.partitions.get_mut(&message.partition).unwrap();
        stream.push(message);
        Ok(())
    }

    fn get_partition_count(&self) -> u16 {
        u16::try_from(self.partitions.len()).unwrap()
    }
}

/// An implementation of [`MessageStorage`] that holds messages in memory.
pub struct MemoryMessageStorage<TPayload> {
    topics: HashMap<Topic, TopicContent<TPayload>>,
}

impl<TPayload> Default for MemoryMessageStorage<TPayload> {
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
            .insert(topic.clone(), TopicContent::new(&topic, partitions));
        Ok(())
    }

    fn list_topics(&self) -> Vec<&Topic> {
        self.topics.keys().collect()
    }

    fn delete_topic(&mut self, topic: &Topic) -> Result<(), TopicDoesNotExist> {
        self.topics.remove(topic).ok_or(TopicDoesNotExist)?;
        Ok(())
    }

    fn get_partition_count(&self, topic: &Topic) -> Result<u16, TopicDoesNotExist> {
        match self.topics.get(topic) {
            Some(x) => Ok(x.get_partition_count()),
            None => Err(TopicDoesNotExist),
        }
    }

    fn consume(
        &self,
        partition: &Partition,
        offset: u64,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumeError> {
        let offset = usize::try_from(offset).unwrap();
        let messages = self.topics[&partition.topic].get_messages(partition)?;
        match messages.len().cmp(&offset) {
            Ordering::Greater => Ok(Some(messages[offset].clone())),
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
        let offset = messages.get_messages(partition)?.len();
        let offset = u64::try_from(offset).unwrap();
        let _ = messages.add_message(BrokerMessage::new(
            payload,
            partition.clone(),
            offset,
            timestamp,
        ));
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryMessageStorage;
    use super::TopicContent;
    use crate::backends::storages::MessageStorage;
    use crate::types::{BrokerMessage, Partition, Topic};
    use chrono::Utc;

    #[test]
    fn test_partition_count() {
        let t = Topic {
            name: "test".to_string(),
        };
        let topic: TopicContent<String> = TopicContent::new(&t, 64);
        assert_eq!(topic.get_partition_count(), 64);
    }

    #[test]
    fn test_empty_partitions() {
        let t = Topic {
            name: "test".to_string(),
        };
        let p1 = Partition {
            topic: t.clone(),
            index: 0,
        };
        let p2 = Partition {
            topic: t.clone(),
            index: 1,
        };
        let topic: TopicContent<String> = TopicContent::new(&t, 2);
        assert_eq!(topic.get_messages(&p1).unwrap().len(), 0);
        assert_eq!(topic.get_messages(&p2).unwrap().len(), 0);
    }

    #[test]
    fn test_invalid_partition() {
        let t = Topic {
            name: "test".to_string(),
        };
        let topic: TopicContent<String> = TopicContent::new(&t, 2);
        let p1 = Partition {
            topic: t,
            index: 10,
        };
        assert!(topic.get_messages(&p1).is_err());
    }

    #[test]
    fn test_add_messages() {
        let t = Topic {
            name: "test".to_string(),
        };
        let mut topic: TopicContent<String> = TopicContent::new(&t, 2);
        let now = Utc::now();
        let p = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };
        let res = topic.add_message(BrokerMessage::new("payload".to_string(), p, 10, now));

        let p0 = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };
        assert!(res.is_ok());
        assert_eq!(topic.get_messages(&p0).unwrap().len(), 1);

        let queue = topic.get_messages(&p0).unwrap();
        assert_eq!(queue[0].offset, 10);

        let p1 = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 1,
        };
        assert_eq!(topic.get_messages(&p1).unwrap().len(), 0);
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
