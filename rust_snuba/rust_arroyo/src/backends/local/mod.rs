pub mod broker;

use super::{AssignmentCallbacks, CommitOffsets, Consumer, ConsumerError};
use crate::types::{BrokerMessage, Partition, Topic};
use broker::LocalBroker;
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RebalanceNotSupported;

enum Callback {
    Assign(HashMap<Partition, u64>),
    Revoke(Vec<Partition>),
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

pub struct LocalConsumer<TPayload, C> {
    id: Uuid,
    group: String,
    broker: LocalBroker<TPayload>,
    pending_callback: VecDeque<Callback>,
    paused: HashSet<Partition>,
    // The offset that a the last ``EndOfPartition`` exception that was
    // raised at. To maintain consistency with the Confluent consumer, this
    // is only sent once per (partition, offset) pair.
    subscription_state: SubscriptionState<C>,
    enable_end_of_partition: bool,
    commit_offset_calls: u32,
    close_calls: u32,
    closed: bool,
}

impl<TPayload, C> LocalConsumer<TPayload, C> {
    pub fn new(
        id: Uuid,
        broker: LocalBroker<TPayload>,
        group: String,
        enable_end_of_partition: bool,
        callbacks: C,
    ) -> Self {
        Self {
            id,
            group,
            broker,
            pending_callback: VecDeque::new(),
            paused: HashSet::new(),
            subscription_state: SubscriptionState {
                topics: Vec::new(),
                callbacks: Some(callbacks),
                offsets: HashMap::new(),
                last_eof_at: HashMap::new(),
            },
            enable_end_of_partition,
            commit_offset_calls: 0,
            close_calls: 0,
            closed: false,
        }
    }

    fn is_subscribed<'p>(&self, mut partitions: impl Iterator<Item = &'p Partition>) -> bool {
        let subscribed = &self.subscription_state.offsets;
        partitions.all(|partition| subscribed.contains_key(partition))
    }
}

impl<TPayload: 'static, C: AssignmentCallbacks> Consumer<TPayload, C>
    for LocalConsumer<TPayload, C>
{
    fn subscribe(&mut self, topics: &[Topic], callbacks: C) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        let offsets = self
            .broker
            .subscribe(self.id, self.group.clone(), topics.to_vec())
            .unwrap();
        self.subscription_state.topics = topics.to_vec();
        self.subscription_state.callbacks = Some(callbacks);

        self.pending_callback.push_back(Callback::Assign(offsets));

        self.subscription_state.last_eof_at.clear();
        Ok(())
    }

    fn unsubscribe(&mut self) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }

        let partitions = self
            .broker
            .unsubscribe(self.id, self.group.clone())
            .unwrap();
        self.pending_callback
            .push_back(Callback::Revoke(partitions));

        self.subscription_state.topics.clear();
        self.subscription_state.last_eof_at.clear();
        Ok(())
    }

    fn poll(
        &mut self,
        _timeout: Option<Duration>,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }

        while !self.pending_callback.is_empty() {
            let callback = self.pending_callback.pop_front().unwrap();
            match callback {
                Callback::Assign(offsets) => {
                    if let Some(callbacks) = self.subscription_state.callbacks.as_mut() {
                        callbacks.on_assign(offsets.clone());
                    }
                    self.subscription_state.offsets = offsets;
                }
                Callback::Revoke(partitions) => {
                    if let Some(callbacks) = self.subscription_state.callbacks.as_mut() {
                        let offset_stage = OffsetCommitter {
                            group: &self.group,
                            broker: &mut self.broker,
                        };
                        callbacks.on_revoke(offset_stage, partitions.clone());
                    }
                    self.subscription_state.offsets = HashMap::new();
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
            let message = self.broker.consume(partition, offset).unwrap();
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
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        if !self.is_subscribed(partitions.iter()) {
            return Err(ConsumerError::EndOfPartition);
        }

        self.paused.extend(partitions);
        Ok(())
    }

    fn resume(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        if !self.is_subscribed(partitions.iter()) {
            return Err(ConsumerError::UnassignedPartition);
        }

        for p in partitions {
            self.paused.remove(&p);
        }
        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        Ok(self.paused.clone())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        Ok(self.subscription_state.offsets.clone())
    }

    fn seek(&self, _: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        unimplemented!("Seek is not implemented");
    }

    fn commit_offsets(&mut self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }

        if !self.is_subscribed(offsets.keys()) {
            return Err(ConsumerError::UnassignedPartition);
        }

        self.broker.commit(&self.group, offsets);
        self.commit_offset_calls += 1;

        Ok(())
    }

    fn close(&mut self) {
        if !self.subscription_state.topics.is_empty() {
            let partitions = self
                .broker
                .unsubscribe(self.id, self.group.clone())
                .unwrap();
            if let Some(c) = self.subscription_state.callbacks.as_mut() {
                let offset_stage = OffsetCommitter {
                    group: &self.group,
                    broker: &mut self.broker,
                };
                c.on_revoke(offset_stage, partitions);
            }
        }
        self.closed = true;
        self.close_calls += 1;
    }

    fn closed(&self) -> bool {
        self.closed
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
            broker,
            "test_group".to_string(),
            true,
            EmptyCallbacks {},
        );
        assert!(consumer.subscription_state.topics.is_empty());

        let res = consumer.subscribe(&[topic1, topic2], EmptyCallbacks {});
        assert!(res.is_ok());
        assert_eq!(consumer.pending_callback.len(), 1);

        let _ = consumer.poll(Some(Duration::from_millis(100)));
        let expected = HashMap::from([
            (Partition::new(topic1, 0), 0),
            (Partition::new(topic1, 1), 0),
            (Partition::new(topic2, 0), 0),
        ]);
        assert_eq!(consumer.subscription_state.offsets, expected);
        assert_eq!(consumer.pending_callback.len(), 0);

        let res = consumer.unsubscribe();
        assert!(res.is_ok());
        assert_eq!(consumer.pending_callback.len(), 1);
        let _ = consumer.poll(Some(Duration::from_millis(100)));
        assert!(consumer.subscription_state.offsets.is_empty());
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
            broker,
            "test_group".to_string(),
            true,
            TheseCallbacks {},
        );

        let _ = consumer.subscribe(&[topic1, topic2], TheseCallbacks {});
        let _ = consumer.poll(Some(Duration::from_millis(100)));

        let _ = consumer.unsubscribe();
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
            broker,
            "test_group".to_string(),
            true,
            TheseCallbacks {},
        );

        let _ = consumer.subscribe(&[topic2], TheseCallbacks {});

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
            broker,
            "test_group".to_string(),
            false,
            EmptyCallbacks {},
        );
        let _ = consumer.subscribe(&[topic2], EmptyCallbacks {});

        assert_eq!(consumer.poll(None).unwrap(), None);
        let _ = consumer.pause(HashSet::from([partition]));
        assert_eq!(consumer.paused().unwrap(), HashSet::from([partition]));

        let _ = consumer.resume(HashSet::from([partition]));
        assert_eq!(consumer.poll(None).unwrap(), None);
    }

    #[test]
    fn test_commit() {
        let broker = build_broker();
        let mut consumer = LocalConsumer::new(
            Uuid::nil(),
            broker,
            "test_group".to_string(),
            false,
            EmptyCallbacks {},
        );
        let topic2 = Topic::new("test2");
        let _ = consumer.subscribe(&[topic2], EmptyCallbacks {});
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
