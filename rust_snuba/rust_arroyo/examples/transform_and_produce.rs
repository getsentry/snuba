extern crate rust_arroyo;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::transform::Transform;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory, InvalidMessage,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Partition, Position, Topic};
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

// struct HashPasswordAndProduceStrategy {
//     partitions: HashMap<Partition, Position>,
// }

fn identity(value: KafkaPayload) -> Result<String, InvalidMessage> {
    print!("transforming value: {:?}", value.payload);
    Ok("password hash".to_string())
}
struct Noop {}
impl ProcessingStrategy<String> for Noop {
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }
    fn submit(&mut self, _message: Message<String>) -> Result<(), MessageRejected> {
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}

struct HashPasswordAndProduceStrategyFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for HashPasswordAndProduceStrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let mut hashPasswordAndProduceStrategy = Transform {
            function: identity,
            next_step: Box::new(Noop {}),
        };
        Box::new(hashPasswordAndProduceStrategy)
    }
}

fn main() {


    // TODO we can get these from the yaml file
    let config = KafkaConfig::new_consumer_config(
        vec!["0.0.0.0:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );


    let consumer = Box::new(KafkaConsumer::new(config));
    let topic = Topic {
        name: "test_static".to_string(),
    };

    let mut processor = StreamProcessor::new(consumer, Box::new(HashPasswordAndProduceStrategyFactory {}));
    processor.subscribe(topic);
    loop {
        println!("running once");
        let _ = processor.run_once();

        //sleep
        std::thread::sleep(std::time::Duration::from_millis(10000));

    }
}
