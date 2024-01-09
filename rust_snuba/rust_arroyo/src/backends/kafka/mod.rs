use self::config::KafkaConsumerConfig;

use super::AssignmentCallbacks;
use super::CommitOffsets;
use super::Consumer as ArroyoConsumer;
use super::ConsumerError;
use crate::backends::kafka::types::KafkaPayload;
use crate::types::{BrokerMessage, Partition, Topic};
use chrono::{DateTime, NaiveDateTime, Utc};
use parking_lot::Mutex;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::{RDKafkaErrorCode, RDKafkaRespErr};
use sentry::Hub;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub mod config;
mod errors;
pub mod producer;
pub mod types;

#[derive(Eq, Hash, PartialEq)]
enum KafkaConsumerState {
    Consuming,
    #[allow(dead_code)]
    Error,
    #[allow(dead_code)]
    Assigning,
    #[allow(dead_code)]
    Revoking,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum InitialOffset {
    Earliest,
    Latest,
    #[default]
    Error,
}

impl fmt::Display for InitialOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InitialOffset::Earliest => write!(f, "earliest"),
            InitialOffset::Latest => write!(f, "latest"),
            InitialOffset::Error => write!(f, "error"),
        }
    }
}

impl FromStr for InitialOffset {
    type Err = ConsumerError;
    fn from_str(auto_offset_reset: &str) -> Result<Self, Self::Err> {
        match auto_offset_reset {
            "earliest" => Ok(InitialOffset::Earliest),
            "latest" => Ok(InitialOffset::Latest),
            "error" => Ok(InitialOffset::Error),
            _ => Err(ConsumerError::InvalidConfig),
        }
    }
}

impl KafkaConsumerState {
    fn assert_consuming_state(&self) -> Result<(), ConsumerError> {
        match self {
            KafkaConsumerState::Error => Err(ConsumerError::ConsumerErrored),
            _ => Ok(()),
        }
    }
}

fn create_kafka_message(topics: &[Topic], msg: BorrowedMessage) -> BrokerMessage<KafkaPayload> {
    let topic = msg.topic();
    // NOTE: We avoid calling `Topic::new` here, as that uses a lock to intern the `topic` name.
    // As we only ever expect one of our pre-defined topics, we can also guard against Broker errors.
    let Some(&topic) = topics.iter().find(|t| t.as_str() == topic) else {
        panic!("Received message for topic `{topic}` that we never subscribed to");
    };
    let partition = Partition {
        topic,
        index: msg.partition() as u16,
    };
    let time_millis = msg.timestamp().to_millis().unwrap_or(0);

    BrokerMessage::new(
        KafkaPayload::new(
            msg.key().map(|k| k.to_vec()),
            msg.headers().map(|h| h.into()),
            msg.payload().map(|p| p.to_vec()),
        ),
        partition,
        msg.offset() as u64,
        DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_millis(time_millis).unwrap_or(NaiveDateTime::MIN),
            Utc,
        ),
    )
}

fn commit_impl<C: AssignmentCallbacks>(
    consumer: &BaseConsumer<CustomContext<C>>,
    offsets: HashMap<Partition, u64>,
) -> Result<(), ConsumerError> {
    let mut partitions = TopicPartitionList::with_capacity(offsets.len());
    for (partition, offset) in &offsets {
        partitions.add_partition_offset(
            partition.topic.as_str(),
            partition.index as i32,
            Offset::from_raw(*offset as i64),
        )?;
    }

    consumer.commit(&partitions, CommitMode::Sync).unwrap();
    Ok(())
}

struct OffsetCommitter<'a, C: AssignmentCallbacks> {
    consumer: &'a BaseConsumer<CustomContext<C>>,
}

impl<'a, C: AssignmentCallbacks> CommitOffsets for OffsetCommitter<'a, C> {
    fn commit(self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        commit_impl(self.consumer, offsets)
    }
}

pub struct CustomContext<C: AssignmentCallbacks> {
    hub: Arc<Hub>,
    callbacks: C,
    offset_state: Arc<Mutex<OffsetState>>,
    initial_offset_reset: InitialOffset,
}

impl<C: AssignmentCallbacks + Send + Sync> ClientContext for CustomContext<C> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        Hub::run(self.hub.clone(), || match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                tracing::error!("librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Warning => {
                tracing::warn!("librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                tracing::info!("librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Debug => {
                tracing::debug!("librdkafka: {fac} {log_message}");
            }
        })
    }

    fn error(&self, error: KafkaError, reason: &str) {
        Hub::run(self.hub.clone(), || {
            let error: &dyn std::error::Error = &error;
            tracing::error!(error, "librdkafka: {error}: {reason}");
        })
    }
}

impl<C: AssignmentCallbacks> ConsumerContext for CustomContext<C> {
    // handle entire rebalancing flow ourselves, so that we can call rdkafka.assign with a
    // customized list of offsets. if we use pre_rebalance and post_rebalance callbacks from
    // rust-rdkafka, consumer.assign will be called *for us*, leaving us with this flow on
    // partition assignment:
    //
    // 1. rdkafka.assign done by rdkafka
    // 2. post_rebalance called
    // 3. post_rebalance modifies the assignment (if e.g. strict_offset_reset=true and
    //    auto_offset_reset=latest)
    // 4. rdkafka.assign is called *again*
    //
    // in comparison, confluent-kafka-python will execute on_assign, and only call rdkafka.assign
    // if the callback did not already explicitly call assign.
    //
    // if we call rdkafka.assign multiple times, we have seen random AutoOffsetReset errors popping
    // up in poll(), since we (briefly) assigned invalid offsets to the consumer
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                let mut partitions: Vec<Partition> = Vec::new();
                let mut offset_state = self.offset_state.lock();
                for partition in tpl.elements().iter() {
                    let topic = Topic::new(partition.topic());
                    let index = partition.partition() as u16;
                    let arroyo_partition = Partition::new(topic, index);

                    if offset_state.offsets.remove(&arroyo_partition).is_none() {
                        tracing::warn!(
                            "failed to delete offset for unknown partition: {}",
                            arroyo_partition
                        );
                    }
                    offset_state.paused.remove(&arroyo_partition);
                    partitions.push(arroyo_partition);
                }

                let committer = OffsetCommitter {
                    consumer: base_consumer,
                };

                // before we give up the assignment, strategies need to flush and commit
                self.callbacks.on_revoke(committer, partitions);

                base_consumer
                    .unassign()
                    .expect("failed to revoke partitions");
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                let committed_offsets = base_consumer
                    .committed_offsets((*tpl).clone(), None)
                    .unwrap();

                let mut offset_map: HashMap<Partition, u64> =
                    HashMap::with_capacity(committed_offsets.count());
                let mut tpl = TopicPartitionList::with_capacity(committed_offsets.count());

                for partition in committed_offsets.elements() {
                    let raw_offset = partition.offset().to_raw().unwrap();

                    let topic = Topic::new(partition.topic());

                    let new_offset = if raw_offset >= 0 {
                        raw_offset
                    } else {
                        // Resolve according to the auto offset reset policy
                        let (low_watermark, high_watermark) = base_consumer
                            .fetch_watermarks(partition.topic(), partition.partition(), None)
                            .unwrap();

                        match self.initial_offset_reset {
                            InitialOffset::Earliest => low_watermark,
                            InitialOffset::Latest => high_watermark,
                            InitialOffset::Error => {
                                panic!("received unexpected offset");
                            }
                        }
                    };

                    offset_map.insert(
                        Partition::new(topic, partition.partition() as u16),
                        new_offset as u64,
                    );

                    tpl.add_partition_offset(
                        partition.topic(),
                        partition.partition(),
                        Offset::from_raw(new_offset),
                    )
                    .unwrap();
                }

                // assign() asap, we can create strategies later
                base_consumer
                    .assign(&tpl)
                    .expect("failed to assign partitions");
                self.offset_state.lock().offsets.extend(&offset_map);

                // Ensure that all partitions are resumed on assignment to avoid
                // carrying over state from a previous assignment.
                base_consumer
                    .resume(&tpl)
                    .expect("failed to resume partitions");

                self.callbacks.on_assign(offset_map);
            }
            _ => {
                let error_code: RDKafkaErrorCode = err.into();
                // We don't panic here since we will likely re-encounter the error on poll
                tracing::error!("Error rebalancing: {}", error_code);
            }
        }
    }
}

#[derive(Default)]
struct OffsetState {
    // offsets: the currently-*read* offset of the consumer, updated on poll()
    // staged_offsets do not exist: the Commit strategy takes care of offset staging
    offsets: HashMap<Partition, u64>,
    // list of partitions that are currently paused
    paused: HashSet<Partition>,
}

pub struct KafkaConsumer<C: AssignmentCallbacks> {
    consumer: BaseConsumer<CustomContext<C>>,
    topics: Vec<Topic>,
    state: KafkaConsumerState,
    offset_state: Arc<Mutex<OffsetState>>,
}

impl<C: AssignmentCallbacks> KafkaConsumer<C> {
    pub fn new(
        config: KafkaConsumerConfig,
        topics: &[Topic],
        callbacks: C,
    ) -> Result<Self, ConsumerError> {
        let offset_state = Arc::new(Mutex::new(OffsetState::default()));

        let initial_offset_reset = config.offset_reset.auto_offset_reset;

        let context = CustomContext {
            hub: Hub::current(),
            callbacks,
            offset_state: offset_state.clone(),
            initial_offset_reset,
        };

        let mut config_obj: ClientConfig = config.into();

        // TODO: Can this actually fail?
        let consumer: BaseConsumer<CustomContext<C>> = config_obj
            .set_log_level(RDKafkaLogLevel::Warning)
            .create_with_context(context)?;

        let topic_str: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        consumer.subscribe(&topic_str)?;
        let topics = topics.to_owned();

        Ok(Self {
            consumer,
            topics,
            state: KafkaConsumerState::Consuming,
            offset_state,
        })
    }

    pub fn shutdown(self) {}
}

impl<C: AssignmentCallbacks> Drop for KafkaConsumer<C> {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}

impl<C: AssignmentCallbacks> ArroyoConsumer<KafkaPayload, C> for KafkaConsumer<C> {
    fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<BrokerMessage<KafkaPayload>>, ConsumerError> {
        self.state.assert_consuming_state()?;

        let duration = timeout.unwrap_or(Duration::ZERO);
        let res = self.consumer.poll(duration);

        match res {
            None => Ok(None),
            Some(res) => {
                let msg = create_kafka_message(&self.topics, res?);
                self.offset_state
                    .lock()
                    .offsets
                    .insert(msg.partition, msg.offset + 1);

                Ok(Some(msg))
            }
        }
    }

    fn pause(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_partition_list = TopicPartitionList::with_capacity(partitions.len());

        {
            let offset_state = self.offset_state.lock();
            let offsets = &offset_state.offsets;
            for partition in &partitions {
                let offset = offsets
                    .get(partition)
                    .ok_or(ConsumerError::UnassignedPartition)?;
                topic_partition_list.add_partition_offset(
                    partition.topic.as_str(),
                    partition.index as i32,
                    Offset::from_raw(*offset as i64),
                )?;
            }
        }

        self.consumer.pause(&topic_partition_list)?;

        {
            let mut offset_state = self.offset_state.lock();
            offset_state.paused.extend(partitions.clone());
        }

        Ok(())
    }

    fn resume(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_partition_list = TopicPartitionList::new();
        let mut to_unpause = Vec::new();
        {
            let offset_state = self.offset_state.lock();
            let offsets = &offset_state.offsets;
            for partition in partitions {
                if !offsets.contains_key(&partition) {
                    return Err(ConsumerError::UnassignedPartition);
                }
                topic_partition_list
                    .add_partition(partition.topic.as_str(), partition.index as i32);
                to_unpause.push(partition);
            }
        }

        self.consumer.resume(&topic_partition_list)?;

        {
            let mut offset_state = self.offset_state.lock();
            for partition in to_unpause {
                offset_state.paused.remove(&partition);
            }
        }

        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        self.state.assert_consuming_state()?;
        Ok(self.offset_state.lock().paused.clone())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        self.state.assert_consuming_state()?;
        Ok(self.offset_state.lock().offsets.clone())
    }

    fn seek(&self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        {
            let offset_state = self.offset_state.lock();
            for key in offsets.keys() {
                if !offset_state.offsets.contains_key(key) {
                    return Err(ConsumerError::UnassignedPartition);
                }
            }
        }

        for (partition, offset) in &offsets {
            self.consumer.seek(
                partition.topic.as_str(),
                partition.index as i32,
                Offset::from_raw(*offset as i64),
                None,
            )?;
        }

        {
            let mut offset_state = self.offset_state.lock();
            offset_state.offsets.extend(offsets);
        }

        Ok(())
    }

    fn commit_offsets(&mut self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;
        commit_impl(&self.consumer, offsets)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{AssignmentCallbacks, InitialOffset, KafkaConsumer};
    use crate::backends::kafka::config::{KafkaConfig, KafkaConsumerConfig};
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::KafkaPayload;
    use crate::backends::{Consumer, Producer};
    use crate::testutils::{get_default_broker, TestTopic};
    use crate::types::{BrokerMessage, Partition, Topic};
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&self, partitions: HashMap<Partition, u64>) {
            println!("assignment event: {:?}", partitions);
        }
        fn on_revoke<C>(&self, _: C, partitions: Vec<Partition>) {
            println!("revocation event: {:?}", partitions);
        }
    }

    fn wait_for_assignments<T: AssignmentCallbacks>(consumer: &mut KafkaConsumer<T>) {
        for _ in 0..10 {
            consumer.poll(Some(Duration::from_millis(5_000))).unwrap();
            if !consumer.tell().unwrap().is_empty() {
                println!("Received assignment");
                break;
            }
            sleep(Duration::from_millis(200));
        }
    }

    fn blocking_poll<T: AssignmentCallbacks>(
        consumer: &mut KafkaConsumer<T>,
    ) -> Option<BrokerMessage<KafkaPayload>> {
        let mut consumer_message = None;

        for _ in 0..10 {
            consumer_message = consumer.poll(Some(Duration::from_millis(5_000))).unwrap();

            if consumer_message.is_some() {
                break;
            }
        }

        consumer_message
    }

    #[test]
    fn test_subscribe() {
        let configuration = KafkaConsumerConfig::new(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group".to_string(),
            InitialOffset::Latest,
            false,
            30_000,
            Default::default(),
        );
        let topic = Topic::new("test");
        KafkaConsumer::new(configuration, &[topic], EmptyCallbacks {}).unwrap();
    }

    #[test]
    fn test_tell() {
        let topic = TestTopic::create("test-tell");
        let configuration = KafkaConsumerConfig::new(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-1".to_string(),
            InitialOffset::Latest,
            false,
            30_000,
            Default::default(),
        );
        let mut consumer =
            KafkaConsumer::new(configuration, &[topic.topic], EmptyCallbacks {}).unwrap();
        assert_eq!(consumer.tell().unwrap(), HashMap::new());

        // Getting the assignment may take a while
        for _ in 0..10 {
            consumer.poll(Some(Duration::from_millis(5_000))).unwrap();
            if consumer.tell().unwrap().len() == 1 {
                println!("Received assignment");
                break;
            }
            sleep(Duration::from_millis(200));
        }

        let offsets = consumer.tell().unwrap();
        // One partition was assigned
        assert_eq!(offsets.len(), 1);
        consumer.shutdown();
    }

    /// check that consumer does not crash with strict_offset_reset if the offset does not exist
    /// yet.
    #[test]
    fn test_offset_reset_strict() {
        let topic = TestTopic::create("test-offset-reset-strict");
        let configuration = KafkaConsumerConfig::new(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-1".to_string(),
            InitialOffset::Earliest,
            true,
            30_000,
            Default::default(),
        );

        let producer_configuration = KafkaConfig::new(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            Default::default(),
        );

        let producer = KafkaProducer::new(producer_configuration);
        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));

        producer
            .produce(&crate::types::TopicOrPartition::Topic(topic.topic), payload)
            .expect("Message produced");

        let mut consumer =
            KafkaConsumer::new(configuration, &[topic.topic], EmptyCallbacks {}).unwrap();
        assert_eq!(consumer.tell().unwrap(), HashMap::new());

        let mut consumer_message = None;

        for _ in 0..10 {
            consumer_message = consumer.poll(Some(Duration::from_millis(5_000))).unwrap();

            if consumer_message.is_some() {
                break;
            }
        }

        let consumer_message = consumer_message.unwrap();

        assert_eq!(consumer_message.offset, 0);
        let consumer_payload = consumer_message.payload.payload().unwrap();
        assert_eq!(consumer_payload, b"asdf");

        assert!(consumer
            .poll(Some(Duration::from_millis(10)))
            .unwrap()
            .is_none());

        consumer
            .commit_offsets(HashMap::from([(
                consumer_message.partition,
                consumer_message.offset + 1,
            )]))
            .unwrap();

        consumer.shutdown();
    }

    #[test]
    fn test_commit() {
        let topic = TestTopic::create("test-commit");
        let configuration = KafkaConsumerConfig::new(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-2".to_string(),
            InitialOffset::Latest,
            false,
            30_000,
            Default::default(),
        );

        let mut consumer =
            KafkaConsumer::new(configuration, &[topic.topic], EmptyCallbacks {}).unwrap();

        let positions = HashMap::from([(
            Partition {
                topic: topic.topic,
                index: 0,
            },
            100,
        )]);

        // Wait until the consumer got an assignment
        for _ in 0..10 {
            consumer.poll(Some(Duration::from_millis(5_000))).unwrap();
            if consumer.tell().unwrap().len() == 1 {
                println!("Received assignment");
                break;
            }
            sleep(Duration::from_millis(200));
        }

        consumer.commit_offsets(positions.clone()).unwrap();
        consumer.shutdown();
    }

    #[test]
    fn test_pause() {
        let topic = TestTopic::create("test-pause");
        let configuration = KafkaConsumerConfig::new(
            vec![get_default_broker()],
            // for this particular test, a separate consumer group is apparently needed, as
            // otherwise random rebalancing events will occur when other tests with the same
            // consumer group (but not the same topic) run at the same time
            "my-group-1-test-pause".to_string(),
            InitialOffset::Earliest,
            true,
            60_000,
            Default::default(),
        );

        let mut consumer =
            KafkaConsumer::new(configuration, &[topic.topic], EmptyCallbacks {}).unwrap();

        wait_for_assignments(&mut consumer);

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        topic.produce(payload);

        let old_offsets = consumer.tell().unwrap();
        assert_eq!(
            old_offsets,
            HashMap::from([(Partition::new(topic.topic, 0), 0)])
        );

        let consumer_message = blocking_poll(&mut consumer).unwrap();

        assert_eq!(consumer_message.offset, 0);
        let consumer_payload = consumer_message.payload.payload().unwrap();
        assert_eq!(consumer_payload, b"asdf");

        // try to reproduce the scenario described at
        // https://github.com/getsentry/arroyo/blob/af4be59fa74acbe00e1cf8dd7921de11acb99509/arroyo/backends/kafka/consumer.py#L503-L504
        // -- "Seeking to a specific partition offset and immediately pausing that partition causes
        // the seek to be ignored for some reason."
        consumer.seek(old_offsets.clone()).unwrap();
        let current_partitions: HashSet<_> = consumer.tell().unwrap().into_keys().collect();
        assert_eq!(current_partitions.len(), 1);
        consumer.pause(current_partitions.clone()).unwrap();
        assert_eq!(
            consumer.tell().unwrap(),
            HashMap::from([(Partition::new(topic.topic, 0), 0)])
        );

        let empty_poll = consumer.poll(Some(Duration::from_secs(5))).unwrap();
        assert!(empty_poll.is_none(), "{:?}", empty_poll);
        consumer.resume(current_partitions).unwrap();

        assert_eq!(consumer.tell().unwrap(), old_offsets);
        assert!(consumer.paused().unwrap().is_empty());

        assert_eq!(
            blocking_poll(&mut consumer)
                .unwrap()
                .payload
                .payload()
                .unwrap(),
            b"asdf"
        );

        consumer.shutdown();
    }
}
