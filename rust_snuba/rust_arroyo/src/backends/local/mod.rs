pub mod broker;

use super::{AssignmentCallbacks, CommitOffsets, Consumer, ConsumerError, Producer, ProducerError};
use crate::types::{BrokerMessage, Partition, Topic, TopicOrPartition};
use broker::LocalBroker;
use rand::prelude::*;
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RebalanceNotSupported;

enum Callback {
    Assign(HashMap<Partition, u64>),
}

struct SubscriptionState<C> {
    topics: Vec<Topic>,
    callbacks: Option<C>,
    offsets: HashMap<Partition, u64>,
    last_eof_at: HashMap<Partition, u64>,
}

struct OffsetCommitter<'a, TPayload> {
    group: &'a str,
    broker: &'a mut LocalBroker<TPayload>,
}

impl<'a, TPayload> CommitOffsets for OffsetCommitter<'a, TPayload> {
    fn commit(self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        self.broker.commit(self.group, offsets);
        Ok(())
    }
}

pub struct LocalConsumer<TPayload, C: AssignmentCallbacks> {
    id: Uuid,
    group: String,
    broker: Arc<Mutex<LocalBroker<TPayload>>>,
    pending_callback: VecDeque<Callback>,
    paused: HashSet<Partition>,
    // The offset that a the last ``EndOfPartition`` exception that was
    // raised at. To maintain consistency with the Confluent consumer, this
    // is only sent once per (partition, offset) pair.
    subscription_state: SubscriptionState<C>,
    enable_end_of_partition: bool,
    commit_offset_calls: u32,
}

impl<TPayload, C: AssignmentCallbacks> LocalConsumer<TPayload, C> {
    pub fn new(
        id: Uuid,
        broker: Arc<Mutex<LocalBroker<TPayload>>>,
        group: String,
        enable_end_of_partition: bool,
        topics: &[Topic],
        callbacks: C,
    ) -> Self {
        let mut ret = Self {
            id,
            group,
            broker,
            pending_callback: VecDeque::new(),
            paused: HashSet::new(),
            subscription_state: SubscriptionState {
                topics: topics.to_vec(),
                callbacks: Some(callbacks),
                offsets: HashMap::new(),
                last_eof_at: HashMap::new(),
            },
            enable_end_of_partition,
            commit_offset_calls: 0,
        };

        let offsets = ret
            .broker
            .lock()
            .unwrap()
            .subscribe(ret.id, ret.group.clone(), topics.to_vec())
            .unwrap();

        ret.pending_callback.push_back(Callback::Assign(offsets));

        ret
    }

    fn is_subscribed<'p>(&self, mut partitions: impl Iterator<Item = &'p Partition>) -> bool {
        let subscribed = &self.subscription_state.offsets;
        partitions.all(|partition| subscribed.contains_key(partition))
    }

    pub fn shutdown(self) {}
}

impl<TPayload: 'static, C: AssignmentCallbacks> Consumer<TPayload, C>
    for LocalConsumer<TPayload, C>
{
    fn poll(
        &mut self,
        _timeout: Option<Duration>,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumerError> {
        while !self.pending_callback.is_empty() {
            let callback = self.pending_callback.pop_front().unwrap();
            match callback {
                Callback::Assign(offsets) => {
                    if let Some(callbacks) = self.subscription_state.callbacks.as_mut() {
                        callbacks.on_assign(offsets.clone());
                    }
                    self.subscription_state.offsets = offsets;
                }
            }
        }

        let keys = self.subscription_state.offsets.keys();
        let mut new_offset: Option<(Partition, u64)> = None;
        let mut ret_message: Option<BrokerMessage<TPayload>> = None;
        for partition in keys {
            if self.paused.contains(partition) {
                continue;
            }

            let offset = self.subscription_state.offsets[partition];
            let message = self
                .broker
                .lock()
                .unwrap()
                .consume(partition, offset)
                .unwrap();
            if let Some(msg) = message {
                new_offset = Some((*partition, msg.offset + 1));
                ret_message = Some(msg);
                break;
            }

            if self.enable_end_of_partition
                && (!self.subscription_state.last_eof_at.contains_key(partition)
                    || offset > self.subscription_state.last_eof_at[partition])
            {
                self.subscription_state
                    .last_eof_at
                    .insert(*partition, offset);
                return Err(ConsumerError::EndOfPartition);
            }
        }

        Ok(new_offset.and_then(|(partition, offset)| {
            self.subscription_state.offsets.insert(partition, offset);
            ret_message
        }))
    }

    fn pause(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        if !self.is_subscribed(partitions.iter()) {
            return Err(ConsumerError::EndOfPartition);
        }

        self.paused.extend(partitions);
        Ok(())
    }

    fn resume(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        if !self.is_subscribed(partitions.iter()) {
            return Err(ConsumerError::UnassignedPartition);
        }

        for p in partitions {
            self.paused.remove(&p);
        }
        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        Ok(self.paused.clone())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        Ok(self.subscription_state.offsets.clone())
    }

    fn seek(&self, _: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        unimplemented!("Seek is not implemented");
    }

    fn commit_offsets(&mut self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        if !self.is_subscribed(offsets.keys()) {
            return Err(ConsumerError::UnassignedPartition);
        }

        self.broker.lock().unwrap().commit(&self.group, offsets);
        self.commit_offset_calls += 1;

        Ok(())
    }
}

impl<TPayload, C: AssignmentCallbacks> Drop for LocalConsumer<TPayload, C> {
    fn drop(&mut self) {
        if !self.subscription_state.topics.is_empty() {
            let broker: &mut LocalBroker<_> = &mut self.broker.lock().unwrap();
            let partitions = broker.unsubscribe(self.id, self.group.clone()).unwrap();

            if let Some(c) = self.subscription_state.callbacks.as_mut() {
                let offset_stage = OffsetCommitter {
                    group: &self.group,
                    broker,
                };
                c.on_revoke(offset_stage, partitions);
            }
        }
    }
}

pub(crate) struct LocalProducer<TPayload> {
    broker: Arc<Mutex<LocalBroker<TPayload>>>,
}

impl<TPayload> LocalProducer<TPayload> {
    pub fn new(broker: Arc<Mutex<LocalBroker<TPayload>>>) -> Self {
        Self { broker }
    }
}

impl<TPayload> Clone for LocalProducer<TPayload> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
        }
    }
}

impl<TPayload: Send + Sync + 'static> Producer<TPayload> for LocalProducer<TPayload> {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: TPayload,
    ) -> Result<(), ProducerError> {
        let mut broker = self.broker.lock().unwrap();
        let partition = match destination {
            TopicOrPartition::Topic(t) => {
                let max_partitions = broker
                    .get_topic_partition_count(t)
                    .map_err(|_| ProducerError::ProducerErrorred)?;
                let partition = thread_rng().gen_range(0..max_partitions);
                Partition::new(*t, partition)
            }
            TopicOrPartition::Partition(p) => *p,
        };

        broker
            .produce(&partition, payload)
            .map_err(|_| ProducerError::ProducerErrorred)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{AssignmentCallbacks, LocalConsumer};
    use crate::backends::local::broker::LocalBroker;
    use crate::backends::storages::memory::MemoryMessageStorage;
    use crate::backends::{CommitOffsets, Consumer};
    use crate::types::{Partition, Topic};
    use crate::utils::clock::SystemClock;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use uuid::Uuid;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&self, _: HashMap<Partition, u64>) {}
        fn on_revoke<C: CommitOffsets>(&self, _: C, _: Vec<Partition>) {}
    }

    fn build_broker() -> LocalBroker<String> {
        let storage: MemoryMessageStorage<String> = Default::default();
        let clock = SystemClock {};
        let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));

        let topic1 = Topic::new("test1");
        let topic2 = Topic::new("test2");

        let _ = broker.create_topic(topic1, 2);
        let _ = broker.create_topic(topic2, 1);
        broker
    }

    #[test]
    fn test_consumer_subscription() {
        let broker = build_broker();

        let topic1 = Topic::new("test1");
        let topic2 = Topic::new("test2");

        let mut consumer = LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            true,
            &[topic1, topic2],
            EmptyCallbacks {},
        );

        assert_eq!(consumer.pending_callback.len(), 1);

        let _ = consumer.poll(Some(Duration::from_millis(100)));
        let expected = HashMap::from([
            (Partition::new(topic1, 0), 0),
            (Partition::new(topic1, 1), 0),
            (Partition::new(topic2, 0), 0),
        ]);
        assert_eq!(consumer.subscription_state.offsets, expected);
        assert_eq!(consumer.pending_callback.len(), 0);
    }

    #[test]
    fn test_subscription_callback() {
        let broker = build_broker();

        let topic1 = Topic::new("test1");
        let topic2 = Topic::new("test2");

        struct TheseCallbacks {}
        impl AssignmentCallbacks for TheseCallbacks {
            fn on_assign(&self, partitions: HashMap<Partition, u64>) {
                let topic1 = Topic::new("test1");
                let topic2 = Topic::new("test2");
                assert_eq!(
                    partitions,
                    HashMap::from([
                        (
                            Partition {
                                topic: topic1,
                                index: 0
                            },
                            0
                        ),
                        (
                            Partition {
                                topic: topic1,
                                index: 1
                            },
                            0
                        ),
                        (
                            Partition {
                                topic: topic2,
                                index: 0
                            },
                            0
                        ),
                    ])
                )
            }

            fn on_revoke<C: CommitOffsets>(&self, _: C, partitions: Vec<Partition>) {
                let topic1 = Topic::new("test1");
                let topic2 = Topic::new("test2");
                assert_eq!(
                    partitions,
                    vec![
                        Partition {
                            topic: topic1,
                            index: 0
                        },
                        Partition {
                            topic: topic1,
                            index: 1
                        },
                        Partition {
                            topic: topic2,
                            index: 0
                        },
                    ]
                );
            }
        }

        let mut consumer = LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            true,
            &[topic1, topic2],
            TheseCallbacks {},
        );

        let _ = consumer.poll(Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_consume() {
        let mut broker = build_broker();

        let topic2 = Topic::new("test2");
        let partition = Partition::new(topic2, 0);
        let _ = broker.produce(&partition, "message1".to_string());
        let _ = broker.produce(&partition, "message2".to_string());

        struct TheseCallbacks {}
        impl AssignmentCallbacks for TheseCallbacks {
            fn on_assign(&self, partitions: HashMap<Partition, u64>) {
                let topic2 = Topic::new("test2");
                assert_eq!(
                    partitions,
                    HashMap::from([(
                        Partition {
                            topic: topic2,
                            index: 0
                        },
                        0
                    ),])
                );
            }
            fn on_revoke<C: CommitOffsets>(&self, _: C, _: Vec<Partition>) {}
        }

        let mut consumer = LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            true,
            &[topic2],
            TheseCallbacks {},
        );

        let msg1 = consumer.poll(Some(Duration::from_millis(100))).unwrap();
        assert!(msg1.is_some());
        let msg_content = msg1.unwrap();
        assert_eq!(msg_content.offset, 0);
        assert_eq!(msg_content.payload, "message1".to_string());

        let msg2 = consumer.poll(Some(Duration::from_millis(100))).unwrap();
        assert!(msg2.is_some());
        let msg_content = msg2.unwrap();
        assert_eq!(msg_content.offset, 1);
        assert_eq!(msg_content.payload, "message2".to_string());

        let ret = consumer.poll(Some(Duration::from_millis(100)));
        assert!(ret.is_err());
    }

    #[test]
    fn test_paused() {
        let broker = build_broker();
        let topic2 = Topic::new("test2");
        let partition = Partition::new(topic2, 0);
        let mut consumer = LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            false,
            &[topic2],
            EmptyCallbacks {},
        );

        assert_eq!(consumer.poll(None).unwrap(), None);
        let _ = consumer.pause(HashSet::from([partition]));
        assert_eq!(consumer.paused().unwrap(), HashSet::from([partition]));

        let _ = consumer.resume(HashSet::from([partition]));
        assert_eq!(consumer.poll(None).unwrap(), None);
    }

    #[test]
    fn test_commit() {
        let broker = build_broker();
        let topic2 = Topic::new("test2");
        let mut consumer = LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            false,
            &[topic2],
            EmptyCallbacks {},
        );
        let _ = consumer.poll(None);
        let positions = HashMap::from([(Partition::new(topic2, 0), 100)]);

        let offsets = consumer.commit_offsets(positions.clone());
        assert!(offsets.is_ok());

        // Stage invalid positions
        let invalid_positions = HashMap::from([(Partition::new(topic2, 1), 100)]);

        let commit_result = consumer.commit_offsets(invalid_positions);
        assert!(commit_result.is_err());
    }
}
