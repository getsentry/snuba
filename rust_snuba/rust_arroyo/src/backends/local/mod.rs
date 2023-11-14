pub mod broker;

use super::{AssignmentCallbacks, Consumer, ConsumerError};
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

struct SubscriptionState {
    topics: Vec<Topic>,
    callbacks: Option<Box<dyn AssignmentCallbacks>>,
    offsets: HashMap<Partition, u64>,
    staged_positions: HashMap<Partition, u64>,
    last_eof_at: HashMap<Partition, u64>,
}

pub struct LocalConsumer<TPayload> {
    id: Uuid,
    group: String,
    broker: LocalBroker<TPayload>,
    pending_callback: VecDeque<Callback>,
    paused: HashSet<Partition>,
    // The offset that a the last ``EndOfPartition`` exception that was
    // raised at. To maintain consistency with the Confluent consumer, this
    // is only sent once per (partition, offset) pair.
    subscription_state: SubscriptionState,
    enable_end_of_partition: bool,
    commit_offset_calls: u32,
    close_calls: u32,
    closed: bool,
}

impl<TPayload> LocalConsumer<TPayload> {
    pub fn new(
        id: Uuid,
        broker: LocalBroker<TPayload>,
        group: String,
        enable_end_of_partition: bool,
    ) -> Self {
        Self {
            id,
            group,
            broker,
            pending_callback: VecDeque::new(),
            paused: HashSet::new(),
            subscription_state: SubscriptionState {
                topics: Vec::new(),
                callbacks: None,
                offsets: HashMap::new(),
                staged_positions: HashMap::new(),
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

impl<TPayload> Consumer<TPayload> for LocalConsumer<TPayload> {
    fn subscribe(
        &mut self,
        topics: &[Topic],
        callbacks: Box<dyn AssignmentCallbacks>,
    ) -> Result<(), ConsumerError> {
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

        self.subscription_state.staged_positions.clear();
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
        self.subscription_state.staged_positions.clear();
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
                    match self.subscription_state.callbacks.as_mut() {
                        None => {}
                        Some(callbacks) => {
                            callbacks.on_assign(offsets.clone());
                        }
                    }
                    self.subscription_state.offsets = offsets;
                }
                Callback::Revoke(partitions) => {
                    match self.subscription_state.callbacks.as_mut() {
                        None => {}
                        Some(callbacks) => {
                            callbacks.on_revoke(partitions.clone());
                        }
                    }
                    self.subscription_state.offsets = HashMap::new();
                }
            }
        }

        let keys = self.subscription_state.offsets.keys();
        let mut new_offset: Option<(Partition, u64)> = None;
        let mut ret_message: Option<BrokerMessage<TPayload>> = None;
        for partition in keys.collect::<Vec<_>>() {
            if self.paused.contains(partition) {
                continue;
            }

            let offset = self.subscription_state.offsets[partition];
            let message = self.broker.consume(partition, offset).unwrap();
            match message {
                Some(msg) => {
                    new_offset = Some((*partition, msg.offset + 1));
                    ret_message = Some(msg);
                    break;
                }
                None => {
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
            }
        }

        match new_offset {
            Some((partition, offset)) => {
                self.subscription_state.offsets.insert(partition, offset);
                Ok(ret_message)
            }
            None => Ok(None),
        }
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

    fn stage_offsets(&mut self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }
        if !self.is_subscribed(offsets.keys()) {
            return Err(ConsumerError::UnassignedPartition);
        }

        self.subscription_state.staged_positions.extend(offsets);
        Ok(())
    }

    fn commit_offsets(&mut self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        if self.closed {
            return Err(ConsumerError::ConsumerClosed);
        }

        let positions = std::mem::take(&mut self.subscription_state.staged_positions);
        self.broker.commit(&self.group, positions.clone());
        self.commit_offset_calls += 1;

        Ok(positions)
    }

    fn close(&mut self) {
        let partitions = self
            .broker
            .unsubscribe(self.id, self.group.clone())
            .unwrap();
        match self.subscription_state.callbacks.as_mut() {
            None => {}
            Some(c) => {
                c.on_revoke(partitions);
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
    use crate::backends::Consumer;
    use crate::types::{Partition, Topic};
    use crate::utils::clock::SystemClock;
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use uuid::Uuid;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
        fn on_revoke(&mut self, _: Vec<Partition>) {}
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

        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(EmptyCallbacks {});
        let mut consumer = LocalConsumer::new(Uuid::nil(), broker, "test_group".to_string(), true);
        assert!(consumer.subscription_state.topics.is_empty());

        let res = consumer.subscribe(&[topic1, topic2], my_callbacks);
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
            fn on_assign(&mut self, partitions: HashMap<Partition, u64>) {
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
            fn on_revoke(&mut self, partitions: Vec<Partition>) {
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

        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(TheseCallbacks {});

        let mut consumer = LocalConsumer::new(Uuid::nil(), broker, "test_group".to_string(), true);

        let _ = consumer.subscribe(&[topic1, topic2], my_callbacks);
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
            fn on_assign(&mut self, partitions: HashMap<Partition, u64>) {
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
            fn on_revoke(&mut self, _: Vec<Partition>) {}
        }

        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(TheseCallbacks {});
        let mut consumer = LocalConsumer::new(Uuid::nil(), broker, "test_group".to_string(), true);

        let _ = consumer.subscribe(&[topic2], my_callbacks);

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
        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(EmptyCallbacks {});
        let mut consumer = LocalConsumer::new(Uuid::nil(), broker, "test_group".to_string(), false);
        let _ = consumer.subscribe(&[topic2], my_callbacks);

        assert_eq!(consumer.poll(None).unwrap(), None);
        let _ = consumer.pause(HashSet::from([partition]));
        assert_eq!(consumer.paused().unwrap(), HashSet::from([partition]));

        let _ = consumer.resume(HashSet::from([partition]));
        assert_eq!(consumer.poll(None).unwrap(), None);
    }

    #[test]
    fn test_commit() {
        let broker = build_broker();
        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(EmptyCallbacks {});
        let mut consumer = LocalConsumer::new(Uuid::nil(), broker, "test_group".to_string(), false);
        let topic2 = Topic::new("test2");
        let _ = consumer.subscribe(&[topic2], my_callbacks);
        let _ = consumer.poll(None);
        let positions = HashMap::from([(Partition::new(topic2, 0), 100)]);
        let stage_result = consumer.stage_offsets(positions.clone());
        assert!(stage_result.is_ok());

        let offsets = consumer.commit_offsets();
        assert!(offsets.is_ok());
        assert_eq!(offsets.unwrap(), positions);

        // Stage invalid positions
        let invalid_positions = HashMap::from([(Partition::new(topic2, 1), 100)]);

        let stage_result = consumer.stage_offsets(invalid_positions);
        assert!(stage_result.is_err());
    }
}
