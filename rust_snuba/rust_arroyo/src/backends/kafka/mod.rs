use super::kafka::config::KafkaConfig;
use super::AssignmentCallbacks;
use super::CommitOffsets;
use super::Consumer as ArroyoConsumer;
use super::ConsumerError;
use crate::backends::kafka::types::KafkaPayload;
use crate::types::{BrokerMessage, Partition, Topic};
use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use sentry::Hub;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
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

fn create_kafka_message(msg: BorrowedMessage) -> BrokerMessage<KafkaPayload> {
    let topic = Topic::new(msg.topic());
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
    // Latest offsets seen by the consumer
    consumer_offsets: Arc<Mutex<HashMap<Partition, u64>>>,
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
    fn pre_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        if let Rebalance::Revoke(list) = rebalance {
            let mut partitions: Vec<Partition> = Vec::new();
            for partition in list.elements().iter() {
                let topic = Topic::new(partition.topic());
                let index = partition.partition() as u16;
                partitions.push(Partition::new(topic, index));
            }

            let mut offsets = self.consumer_offsets.lock().unwrap();
            for partition in partitions.iter() {
                offsets.remove(partition);
            }

            let committer = OffsetCommitter {
                consumer: base_consumer,
            };

            self.callbacks.on_revoke(committer, partitions);
        }
    }

    fn post_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        if let Rebalance::Assign(list) = rebalance {
            let committed_offsets = base_consumer
                .committed_offsets((*list).clone(), None)
                .unwrap();

            let mut offset_map: HashMap<Partition, u64> = HashMap::new();

            for partition in committed_offsets.elements() {
                let raw_offset = partition.offset().to_raw().unwrap();

                let topic = Topic::new(partition.topic());

                if raw_offset >= 0 {
                    offset_map.insert(
                        Partition::new(topic, partition.partition() as u16),
                        raw_offset as u64,
                    );
                } else {
                    // Resolve according to the auto offset reset policy
                    let (low_watermark, high_watermark) = base_consumer
                        .fetch_watermarks(partition.topic(), partition.partition(), None)
                        .unwrap();

                    let resolved_offset = match self.initial_offset_reset {
                        InitialOffset::Earliest => low_watermark,
                        InitialOffset::Latest => high_watermark,
                        InitialOffset::Error => {
                            panic!("received unexpected offset");
                        }
                    };
                    offset_map.insert(
                        Partition::new(topic, partition.partition() as u16),
                        resolved_offset as u64,
                    );
                }
            }

            let mut tpl = TopicPartitionList::with_capacity(offset_map.len());
            for (partition, offset) in &offset_map {
                tpl.add_partition_offset(
                    partition.topic.as_str(),
                    partition.index as i32,
                    Offset::from_raw(*offset as i64),
                )
                .unwrap();
            }

            // specifically in rust-rdkafka (not in python confluent-kafka) we are observing a
            // weird bug where assigning an offset after rebalancing does not work until the
            // consumer is being polled.
            //
            // the poll() may return an offset-related error or advance the internal rdkafka
            // buffer. but it shouldn't matter which case we hit, because even if poll() returns a
            // message here, we are resetting offsets to something else, and (hopefully) not commit
            // anything before that
            if let Some(Err(err)) = base_consumer.poll(Some(Duration::from_millis(100))) {
                if matches!(
                    err.rdkafka_error_code(),
                    Some(RDKafkaErrorCode::AutoOffsetReset)
                ) {
                    tracing::info!("polling failed during rebalancing, resetting offsets manually");
                } else {
                    panic!("consumer poll failed in callback: {}", err);
                }
            }

            base_consumer
                .assign(&tpl)
                .expect("failed to assign partitions");
            self.consumer_offsets.lock().unwrap().extend(&offset_map);

            // Ensure that all partitions are resumed on assignment to avoid
            // carrying over state from a previous assignment.
            base_consumer
                .resume(&tpl)
                .expect("failed to resume partitions");

            self.callbacks.on_assign(offset_map);
        }
    }
}

pub struct KafkaConsumer<C: AssignmentCallbacks> {
    // TODO: This has to be an option as of now because rdkafka requires
    // callbacks during the instantiation. While the streaming processor
    // can only pass the callbacks during the subscribe call.
    // So we need to build the kafka consumer upon subscribe and not
    // in the constructor.
    pub consumer: BaseConsumer<CustomContext<C>>,
    state: KafkaConsumerState,
    offsets: Arc<Mutex<HashMap<Partition, u64>>>,
}

impl<C: AssignmentCallbacks> KafkaConsumer<C> {
    pub fn new(config: KafkaConfig, topics: &[Topic], callbacks: C) -> Result<Self, ConsumerError> {
        let offsets = Arc::new(Mutex::new(HashMap::new()));

        let initial_offset_reset = config
            .offset_reset_config()
            .ok_or(ConsumerError::InvalidConfig)?
            .auto_offset_reset;

        let context = CustomContext {
            hub: Hub::current(),
            callbacks,
            consumer_offsets: offsets.clone(),
            initial_offset_reset,
        };

        let mut config_obj: ClientConfig = config.into();

        // TODO: Can this actually fail?
        let consumer: BaseConsumer<CustomContext<C>> = config_obj
            .set_log_level(RDKafkaLogLevel::Warning)
            .create_with_context(context)?;

        let topic_str: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        consumer.subscribe(&topic_str)?;

        Ok(Self {
            consumer,
            state: KafkaConsumerState::Consuming,
            offsets,
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

        ////////
        if let Some(Err(err)) = &res {
            if matches!(
                err.rdkafka_error_code(),
                Some(RDKafkaErrorCode::AutoOffsetReset)
            ) {
                tracing::info!("polling failed, resetting offsets manually");
                if !self.offsets.lock().unwrap().is_empty() {
                    let mut tpl =
                        TopicPartitionList::with_capacity(self.offsets.lock().unwrap().len());
                    let offsets = self.offsets.lock().unwrap();
                    for (partition, offset) in offsets.iter() {
                        tpl.add_partition_offset(
                            partition.topic.as_str(),
                            partition.index as i32,
                            Offset::from_raw(*offset as i64),
                        )?;
                    }

                    self.consumer.assign(&tpl).expect("failed to assign");
                }
            }

            // return Ok(None);
        }
        ///////

        match res {
            None => Ok(None),
            Some(res) => {
                let msg = create_kafka_message(res?);
                self.offsets
                    .lock()
                    .unwrap()
                    .insert(msg.partition, msg.offset);

                Ok(Some(msg))
            }
        }
    }

    fn pause(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_partition_list = TopicPartitionList::with_capacity(partitions.len());
        {
            let offsets = self.offsets.lock().unwrap();
            for partition in partitions {
                let offset = offsets
                    .get(&partition)
                    .ok_or(ConsumerError::UnassignedPartition)?;
                topic_partition_list.add_partition_offset(
                    partition.topic.as_str(),
                    partition.index as i32,
                    Offset::from_raw(*offset as i64),
                )?;
            }
        }

        self.consumer.pause(&topic_partition_list)?;

        Ok(())
    }

    fn resume(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_partition_list = TopicPartitionList::new();
        for partition in partitions {
            if !self.offsets.lock().unwrap().contains_key(&partition) {
                return Err(ConsumerError::UnassignedPartition);
            }
            topic_partition_list.add_partition(partition.topic.as_str(), partition.index as i32);
        }

        self.consumer.resume(&topic_partition_list)?;

        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        //TODO: Implement this
        Ok(HashSet::new())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        self.state.assert_consuming_state()?;
        Ok(self.offsets.lock().unwrap().clone())
    }

    fn seek(&self, _: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        //TODO: Implement this
        Ok(())
    }

    fn commit_offsets(&mut self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;
        commit_impl(&self.consumer, offsets)
    }
}

#[cfg(test)]
mod tests {
    use super::{AssignmentCallbacks, InitialOffset, KafkaConsumer};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::KafkaPayload;
    use crate::backends::{Consumer, Producer};
    use crate::types::{Partition, Topic};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::runtime::Runtime;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&self, _: HashMap<Partition, u64>) {}
        fn on_revoke<C>(&self, _: C, _: Vec<Partition>) {}
    }

    fn get_admin_client() -> AdminClient<DefaultClientContext> {
        let mut config = ClientConfig::new();
        config.set(
            "bootstrap.servers".to_string(),
            std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string()),
        );

        config.create().unwrap()
    }

    async fn create_topic(topic_name: &str, partition_count: i32) {
        let client = get_admin_client();
        let topics = [NewTopic::new(
            topic_name,
            partition_count,
            TopicReplication::Fixed(1),
        )];
        client
            .create_topics(&topics, &AdminOptions::new())
            .await
            .unwrap();
    }

    async fn delete_topic(topic_name: &str) {
        let client = get_admin_client();
        client
            .delete_topics(&[topic_name], &AdminOptions::new())
            .await
            .unwrap();
    }

    struct TestTopic {
        runtime: Runtime,
        topic: Topic,
    }

    impl TestTopic {
        fn create(name: &str) -> Self {
            let runtime = Runtime::new().unwrap();
            let name = format!("rust-arroyo-{}-{}", name, uuid::Uuid::new_v4());
            runtime.block_on(create_topic(&name, 1));
            Self {
                runtime,
                topic: Topic::new(&name),
            }
        }
    }

    impl Drop for TestTopic {
        fn drop(&mut self) {
            let name = self.topic.as_str();
            // i really wish i had async drop now
            self.runtime.block_on(delete_topic(name));
        }
    }

    #[test]
    fn test_subscribe() {
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group".to_string(),
            InitialOffset::Latest,
            false,
            30_000,
            None,
        );
        let topic = Topic::new("test");
        KafkaConsumer::new(configuration, &[topic], EmptyCallbacks {}).unwrap();
    }

    #[test]
    fn test_tell() {
        let topic = TestTopic::create("test-tell");
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-1".to_string(),
            InitialOffset::Latest,
            false,
            30_000,
            None,
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
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-1".to_string(),
            InitialOffset::Earliest,
            true,
            30_000,
            None,
        );

        let producer_configuration = KafkaConfig::new_producer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            None,
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
        // ensure that our weird workarounds in consumer callbacks (polling the consumer and
        // discarding the result) do not read past the message we produced into the new topic
        assert_eq!(consumer_payload, b"asdf");

        assert!(consumer
            .poll(Some(Duration::from_millis(10)))
            .unwrap()
            .is_none());

        let offsets = consumer.tell().unwrap();
        assert_eq!(
            offsets,
            HashMap::from([(Partition::new(topic.topic, 0), 0)])
        );

        consumer
            .commit_offsets(HashMap::from([(
                consumer_message.partition,
                consumer_message.offset + 1,
            )]))
            .unwrap();

        let offsets = consumer.tell().unwrap();
        assert_eq!(
            offsets,
            HashMap::from([(Partition::new(topic.topic, 0), 1)])
        );

        consumer.shutdown();
    }

    #[test]
    fn test_commit() {
        let topic = TestTopic::create("test-commit");
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-2".to_string(),
            InitialOffset::Latest,
            false,
            30_000,
            None,
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
    fn test_pause() {}

    #[test]
    fn test_resume() {}
}
