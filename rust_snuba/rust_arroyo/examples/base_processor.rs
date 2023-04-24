extern crate rust_arroyo;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::{
    commit_offsets, ProcessingStrategy, ProcessingStrategyFactory,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::Topic;
use std::time::Duration;

struct TestFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for TestFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(commit_offsets::new(Duration::from_secs(1)))
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
