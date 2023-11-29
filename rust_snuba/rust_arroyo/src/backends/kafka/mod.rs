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
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub mod config;
mod errors;
pub mod producer;
pub mod types;

#[derive(Eq, Hash, PartialEq)]
enum KafkaConsumerState {
    NotSubscribed,
    Consuming,
    #[allow(dead_code)]
    Error,
    Closed,
    #[allow(dead_code)]
    Assigning,
    #[allow(dead_code)]
    Revoking,
}

impl KafkaConsumerState {
    fn assert_consuming_state(&self) -> Result<(), ConsumerError> {
        match self {
            KafkaConsumerState::Closed => Err(ConsumerError::ConsumerClosed),
            KafkaConsumerState::NotSubscribed => Err(ConsumerError::NotSubscribed),
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

struct OffsetCommitter<'a, C: AssignmentCallbacks>(&'a BaseConsumer<CustomContext<C>>);

impl<'a, C: AssignmentCallbacks> CommitOffsets for OffsetCommitter<'a, C> {
    fn commit(self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        let mut partitions = TopicPartitionList::with_capacity(offsets.len());
        for (partition, offset) in offsets {
            partitions.add_partition_offset(
                partition.topic.as_str(),
                partition.index as i32,
                Offset::from_raw(offset as i64),
            )?;
        }

        self.0.commit(&partitions, CommitMode::Sync).unwrap();

        Ok(())
    }
}

pub struct CustomContext<C: AssignmentCallbacks> {
    callbacks: C,
    consumer_offsets: Arc<Mutex<HashMap<Partition, u64>>>,
}

impl<C: AssignmentCallbacks + Send + Sync> ClientContext for CustomContext<C> {}

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

            self.callbacks
                .on_revoke(OffsetCommitter(base_consumer), partitions);
        }
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        if let Rebalance::Assign(list) = rebalance {
            let mut map: HashMap<Partition, u64> = HashMap::new();
            for partition in list.elements().iter() {
                let topic = Topic::new(partition.topic());
                let index = partition.partition() as u16;
                let offset = partition.offset().to_raw().unwrap();
                map.insert(Partition::new(topic, index), offset as u64);
            }
            let mut offsets = self.consumer_offsets.lock().unwrap();
            for (partition, offset) in &map {
                offsets.insert(*partition, *offset);
            }
            self.callbacks.on_assign(map);
        }
    }

    fn commit_callback(&self, _: KafkaResult<()>, _offsets: &TopicPartitionList) {}
}

pub struct KafkaConsumer<C: AssignmentCallbacks> {
    // TODO: This has to be an option as of now because rdkafka requires
    // callbacks during the instantiation. While the streaming processor
    // can only pass the callbacks during the subscribe call.
    // So we need to build the kafka consumer upon subscribe and not
    // in the constructor.
    pub consumer: Option<BaseConsumer<CustomContext<C>>>,
    config: KafkaConfig,
    state: KafkaConsumerState,
    offsets: Arc<Mutex<HashMap<Partition, u64>>>,
}

impl<C: AssignmentCallbacks> KafkaConsumer<C> {
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            consumer: None,
            config,
            state: KafkaConsumerState::NotSubscribed,
            offsets: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<C: AssignmentCallbacks> ArroyoConsumer<KafkaPayload, C> for KafkaConsumer<C> {
    fn subscribe(&mut self, topics: &[Topic], callbacks: C) -> Result<(), ConsumerError> {
        let context = CustomContext {
            callbacks,
            consumer_offsets: self.offsets.clone(),
        };

        let mut config_obj: ClientConfig = self.config.clone().into();

        let consumer: BaseConsumer<CustomContext<C>> = config_obj
            .set_log_level(RDKafkaLogLevel::Warning)
            .create_with_context(context)?;

        let topic_str: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        consumer.subscribe(&topic_str)?;

        self.consumer = Some(consumer);
        self.state = KafkaConsumerState::Consuming;
        Ok(())
    }

    fn unsubscribe(&mut self) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;
        self.consumer.as_ref().unwrap().unsubscribe();

        Ok(())
    }

    fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<BrokerMessage<KafkaPayload>>, ConsumerError> {
        self.state.assert_consuming_state()?;

        let duration = timeout.unwrap_or(Duration::ZERO);
        let res = self.consumer.as_ref().unwrap().poll(duration);

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

        self.consumer
            .as_ref()
            .unwrap()
            .pause(&topic_partition_list)?;

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

        self.consumer
            .as_ref()
            .unwrap()
            .resume(&topic_partition_list)?;

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

        let mut partitions = TopicPartitionList::with_capacity(offsets.len());
        for (partition, offset) in &offsets {
            partitions.add_partition_offset(
                partition.topic.as_str(),
                partition.index as i32,
                Offset::from_raw(*offset as i64),
            )?;
        }

        self.consumer
            .as_ref()
            .unwrap()
            .commit(&partitions, CommitMode::Sync)
            .unwrap();

        Ok(())
    }

    fn close(&mut self) {
        self.state = KafkaConsumerState::Closed;
        self.consumer = None;
    }

    fn closed(&self) -> bool {
        self.state == KafkaConsumerState::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::{AssignmentCallbacks, KafkaConsumer};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::Consumer;
    use crate::types::{Partition, Topic};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

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

    #[test]
    fn test_subscribe() {
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group".to_string(),
            "latest".to_string(),
            false,
            None,
        );
        let mut consumer = KafkaConsumer::new(configuration);
        let topic = Topic::new("test");
        let my_callbacks = EmptyCallbacks {};
        consumer.subscribe(&[topic], my_callbacks).unwrap();
    }

    #[tokio::test]
    async fn test_tell() {
        create_topic("test", 1).await;
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-1".to_string(),
            "latest".to_string(),
            false,
            None,
        );
        let mut consumer = KafkaConsumer::new(configuration);
        let topic = Topic::new("test");
        assert!(consumer.tell().is_err()); // Not subscribed yet
        consumer.subscribe(&[topic], EmptyCallbacks {}).unwrap();
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
        assert!(offsets.len() == 1);
        consumer.unsubscribe().unwrap();
        consumer.close();

        delete_topic("test").await;
    }

    #[tokio::test]
    async fn test_commit() {
        create_topic("test2", 1).await;
        let configuration = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my-group-2".to_string(),
            "latest".to_string(),
            false,
            None,
        );

        let mut consumer = KafkaConsumer::new(configuration);
        let topic = Topic::new("test2");

        consumer.subscribe(&[topic], EmptyCallbacks {}).unwrap();

        let positions = HashMap::from([(Partition { topic, index: 0 }, 100)]);

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
        consumer.unsubscribe().unwrap();
        consumer.close();
        delete_topic("test2").await;
    }

    #[test]
    fn test_pause() {}

    #[test]
    fn test_resume() {}
}
