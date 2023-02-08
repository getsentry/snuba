extern crate rust_snuba;

use rust_snuba::backends::kafka::config::KafkaConfig;
use rust_snuba::backends::kafka::types::KafkaPayload;
use rust_snuba::backends::kafka::KafkaConsumer;
use rust_snuba::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
};
use rust_snuba::processing::StreamProcessor;
use rust_snuba::types::{Message, Partition, Position, Topic};
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

fn main() {
    let config = KafkaConfig::new_consumer_config(
        vec!["localhost:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );
    let consumer = Box::new(KafkaConsumer::new(config));
    let topic = Topic {
        name: "test_static".to_string(),
    };

    let mut processor = StreamProcessor::new(consumer, Box::new(TestFactory {}));
    processor.subscribe(topic);
    for _ in 0..20 {
        let _ = processor.run_once();
    }
}
