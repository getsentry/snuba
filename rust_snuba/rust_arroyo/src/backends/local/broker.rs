use crate::backends::storages::{ConsumeError, MessageStorage, TopicDoesNotExist, TopicExists};
use crate::types::{BrokerMessage, Partition, Topic};
use crate::utils::clock::Clock;
use chrono::DateTime;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use uuid::Uuid;

pub struct LocalBroker<TPayload: Clone> {
    storage: Box<dyn MessageStorage<TPayload>>,
    clock: Box<dyn Clock>,
    offsets: HashMap<String, HashMap<Partition, u64>>,
    subscriptions: HashMap<String, HashMap<Uuid, Vec<Topic>>>,
}

#[derive(Error, Debug, Clone)]
#[error(transparent)]
pub enum BrokerError {
    #[error("Partition does not exist")]
    PartitionDoesNotExist,

    #[error("Rebalance not supported")]
    RebalanceNotSupported,

    #[error("Topic does not exist")]
    TopicDoesNotExist,
}

impl From<TopicDoesNotExist> for BrokerError {
    fn from(_: TopicDoesNotExist) -> Self {
        BrokerError::TopicDoesNotExist
    }
}

impl<TPayload: Clone> LocalBroker<TPayload> {
    pub fn new(storage: Box<dyn MessageStorage<TPayload>>, clock: Box<dyn Clock>) -> Self {
        Self {
            storage,
            clock,
            offsets: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    pub fn create_topic(&mut self, topic: Topic, partitions: u16) -> Result<(), TopicExists> {
        self.storage.create_topic(topic, partitions)
    }

    pub fn get_topic_partition_count(self, topic: &Topic) -> Result<u16, TopicDoesNotExist> {
        self.storage.get_partition_count(topic)
    }

    pub fn produce(
        &mut self,
        partition: &Partition,
        payload: TPayload,
    ) -> Result<u64, ConsumeError> {
        let time = self.clock.time();
        self.storage
            .produce(partition, payload, DateTime::from(time))
    }

    pub fn subscribe(
        &mut self,
        consumer_id: Uuid,
        consumer_group: String,
        topics: Vec<Topic>,
    ) -> Result<HashMap<Partition, u64>, BrokerError> {
        // Handle rebalancing request which is not supported
        let group_subscriptions = self.subscriptions.get(&consumer_group);
        if let Some(group_s) = group_subscriptions {
            let consumer_subscription = group_s.get(&consumer_id);
            if let Some(consume_subs) = consumer_subscription {
                let subscribed_topics = consume_subs;
                let mut non_matches = subscribed_topics
                    .iter()
                    .zip(&topics)
                    .filter(|&(a, b)| a.name != b.name);
                if non_matches.next().is_some() {
                    return Err(BrokerError::RebalanceNotSupported);
                }
            } else {
                return Err(BrokerError::RebalanceNotSupported);
            }
        }

        let mut assignments = HashMap::new();
        let mut assigned_topics = HashSet::new();

        for topic in topics.iter() {
            if !assigned_topics.contains(topic) {
                assigned_topics.insert(topic);
                let partition_count = self.storage.get_partition_count(topic)?;
                if !self.offsets.contains_key(&consumer_group) {
                    self.offsets.insert(consumer_group.clone(), HashMap::new());
                }
                for n in 0..partition_count {
                    let p = self.storage.get_partition(topic, n).unwrap();
                    let offset = match self.offsets[&consumer_group].get(&p) {
                        None => 0,
                        Some(x) => *x,
                    };
                    assignments.insert(p, offset);
                }
            }
        }

        let group_subscriptions = self.subscriptions.get_mut(&consumer_group);
        match group_subscriptions {
            None => {
                let mut new_group_subscriptions = HashMap::new();
                new_group_subscriptions.insert(consumer_id, topics);
                self.subscriptions
                    .insert(consumer_group.clone(), new_group_subscriptions);
            }
            Some(group_subscriptions) => {
                group_subscriptions.insert(consumer_id, topics);
            }
        }
        Ok(assignments)
    }

    pub fn unsubscribe(&mut self, id: Uuid, group: String) -> Result<Vec<Partition>, BrokerError> {
        let mut ret_partitions = Vec::new();
        let group_subscriptions = self.subscriptions.get_mut(&group).unwrap();
        let subscribed_topics = group_subscriptions.get(&id).unwrap();
        for topic in subscribed_topics.iter() {
            let partitions = self.storage.get_partition_count(topic)?;
            for n in 0..partitions {
                ret_partitions.push(Partition {
                    topic: topic.clone(),
                    index: n,
                });
            }
        }
        group_subscriptions.remove(&id);
        Ok(ret_partitions)
    }

    pub fn consume(
        &self,
        partition: &Partition,
        offset: u64,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumeError> {
        self.storage.consume(partition, offset)
    }

    pub fn commit(&mut self, consumer_group: &str, offsets: HashMap<Partition, u64>) {
        self.offsets.insert(consumer_group.to_string(), offsets);
    }
}

#[cfg(test)]
mod tests {
    use super::LocalBroker;
    use crate::backends::storages::memory::MemoryMessageStorage;
    use crate::types::{Partition, Topic};
    use crate::utils::clock::SystemClock;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn test_topic_creation() {
        let storage: MemoryMessageStorage<String> = Default::default();
        let clock = SystemClock {};
        let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));

        let topic = Topic {
            name: "test".to_string(),
        };
        let res = broker.create_topic(topic.clone(), 16);
        assert!(res.is_ok());

        let res2 = broker.create_topic(topic.clone(), 16);
        assert!(res2.is_err());

        let partitions = broker.get_topic_partition_count(&topic);
        assert_eq!(partitions.unwrap(), 16);
    }

    #[test]
    fn test_produce_consume() {
        let storage: MemoryMessageStorage<String> = Default::default();
        let clock = SystemClock {};
        let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));

        let partition = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };
        let _ = broker.create_topic(
            Topic {
                name: "test".to_string(),
            },
            1,
        );
        let r_prod = broker.produce(&partition, "message".to_string());
        assert!(r_prod.is_ok());
        assert_eq!(r_prod.unwrap(), 0);

        let message = broker.consume(&partition, 0).unwrap().unwrap();
        assert_eq!(message.offset, 0);
        assert_eq!(message.partition, partition.clone());
        assert_eq!(message.payload, "message".to_string());
    }

    fn build_broker() -> LocalBroker<String> {
        let storage: MemoryMessageStorage<String> = Default::default();
        let clock = SystemClock {};
        let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));

        let topic1 = Topic {
            name: "test1".to_string(),
        };
        let topic2 = Topic {
            name: "test2".to_string(),
        };

        let _ = broker.create_topic(topic1, 2);
        let _ = broker.create_topic(topic2, 1);
        broker
    }

    #[test]
    fn test_assignment() {
        let mut broker = build_broker();

        let topic1 = Topic {
            name: "test1".to_string(),
        };
        let topic2 = Topic {
            name: "test2".to_string(),
        };

        let r_assignments = broker.subscribe(
            Uuid::nil(),
            "group".to_string(),
            vec![topic1.clone(), topic2.clone()],
        );
        assert!(r_assignments.is_ok());
        let expected = HashMap::from([
            (
                Partition {
                    topic: topic1.clone(),
                    index: 0,
                },
                0,
            ),
            (
                Partition {
                    topic: topic1.clone(),
                    index: 1,
                },
                0,
            ),
            (
                Partition {
                    topic: topic2.clone(),
                    index: 0,
                },
                0,
            ),
        ]);
        assert_eq!(r_assignments.unwrap(), expected);

        let unassignmnts = broker.unsubscribe(Uuid::nil(), "group".to_string());
        assert!(unassignmnts.is_ok());
        let expected = vec![
            Partition {
                topic: topic1.clone(),
                index: 0,
            },
            Partition {
                topic: topic1,
                index: 1,
            },
            Partition {
                topic: topic2,
                index: 0,
            },
        ];
        assert_eq!(unassignmnts.unwrap(), expected);
    }
}
