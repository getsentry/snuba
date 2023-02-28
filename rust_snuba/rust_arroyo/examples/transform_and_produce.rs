// An example of using the Transform and Produce strategies together.
// inspired by https://github.com/getsentry/arroyo/blob/main/examples/transform_and_produce/script.py
extern crate rust_arroyo;

use rdkafka::message::ToBytes;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::produce::Produce;
use rust_arroyo::processing::strategies::transform::Transform;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory, InvalidMessage,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Partition, Position, Topic, TopicOrPartition};
use std::collections::HashMap;
use std::time::Duration;

struct TestStrategy {
    partitions: HashMap<Partition, Position>,
}
impl ProcessingStrategy<KafkaPayload> for TestStrategy {
    fn poll(&mut self) -> Option<CommitRequest> {
        println!("POLL");
        if !self.partitions.is_empty() {
            // TODO: Actually make commit work. It does not seem
            // to work now.
            let ret = Some(CommitRequest {
                positions: self.partitions.clone(),
            });
            self.partitions.clear();
            ret
        } else {
            None
        }
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        println!("SUBMIT {}", message);
        self.partitions.insert(
            message.partition,
            Position {
                offset: message.offset,
                timestamp: message.timestamp,
            },
        );
        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}

struct TestFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for TestFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(TestStrategy {
            partitions: HashMap::new(),
        })
    }
}

fn reverse_string(value: KafkaPayload) -> Result<KafkaPayload, InvalidMessage> {
    let payload = value.payload.unwrap();
    let str_payload = std::str::from_utf8(&payload).unwrap();
    println!("transforming value: {:?}", str_payload);

    let result_str = str_payload.chars().rev().collect::<String>();

    let result = KafkaPayload {
        payload: Some(result_str.to_bytes().to_vec()),
        ..value
    };
    Ok(result)
}
struct Noop {}
impl ProcessingStrategy<KafkaPayload> for Noop {
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }
    fn submit(&mut self, _message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}



fn main() {
    struct HashPasswordAndProduceStrategyFactory {
        config: KafkaConfig,
        topic: Topic,
    }
    impl ProcessingStrategyFactory<KafkaPayload> for HashPasswordAndProduceStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let producer = KafkaProducer::new(self.config.clone());
            let topic = TopicOrPartition::Topic(self.topic.clone());
            let hash_password_and_produce_strategy = Transform {
                function: reverse_string,
                next_step: Box::new(Produce::new(producer, Box::new(Noop {}), topic ))
            };
            Box::new(hash_password_and_produce_strategy)
        }
    }

    // TODO we can get these from the yaml file
    let config = KafkaConfig::new_consumer_config(
        vec!["0.0.0.0:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );


    let consumer = Box::new(KafkaConsumer::new(config.clone()));
    let mut processor = StreamProcessor::new(consumer,
        Box::new(HashPasswordAndProduceStrategyFactory {
                            config: config.clone(),
                            topic: Topic { name: "test_out".to_string() }
                         }));
    processor.subscribe(Topic {
        name: "test_in".to_string(),
    });
    println!("running processor. transforming from test_in to test_out");
    processor.run();
}
