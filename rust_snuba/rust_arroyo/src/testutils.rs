use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use tokio::runtime::Runtime;

use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::processing::strategies::{CommitRequest, PollError, ProcessingStrategy, SubmitError};
use crate::types::Message;
use crate::types::Topic;

#[derive(Clone)]
pub struct TestStrategy<T> {
    pub messages: Arc<Mutex<Vec<Message<T>>>>,
}

impl<T> Default for TestStrategy<T> {
    fn default() -> Self {
        TestStrategy {
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T> TestStrategy<T> {
    pub fn new() -> Self {
        TestStrategy::default()
    }
}

impl<T: Send> ProcessingStrategy<T> for TestStrategy<T> {
    fn poll(&mut self) -> Result<Option<CommitRequest>, PollError> {
        Ok(None)
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }

    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Result<Option<CommitRequest>, PollError> {
        Ok(None)
    }
}

pub fn get_default_broker() -> String {
    std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())
}

fn get_admin_client() -> AdminClient<DefaultClientContext> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers".to_string(), get_default_broker());

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

pub struct TestTopic {
    runtime: Runtime,
    pub topic: Topic,
}

impl TestTopic {
    pub fn create(name: &str) -> Self {
        let runtime = Runtime::new().unwrap();
        let name = format!("rust-arroyo-{}-{}", name, uuid::Uuid::new_v4());
        runtime.block_on(create_topic(&name, 1));
        Self {
            runtime,
            topic: Topic::new(&name),
        }
    }

    pub fn produce(&self, payload: KafkaPayload) {
        let producer_configuration =
            KafkaConfig::new_producer_config(vec![get_default_broker()], None);

        let producer = KafkaProducer::new(producer_configuration);

        producer
            .produce(&crate::types::TopicOrPartition::Topic(self.topic), payload)
            .expect("Message produced");
    }
}

impl Drop for TestTopic {
    fn drop(&mut self) {
        let name = self.topic.as_str();
        // i really wish i had async drop now
        self.runtime.block_on(delete_topic(name));
    }
}
