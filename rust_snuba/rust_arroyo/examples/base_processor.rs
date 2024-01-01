extern crate rust_arroyo;

use chrono::Duration;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::InitialOffset;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::Topic;

struct TestFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for TestFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(CommitOffsets::new(Duration::seconds(1)))
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let config = KafkaConfig::new_consumer_config(
        vec!["127.0.0.1:9092".to_string()],
        "my_group".to_string(),
        InitialOffset::Latest,
        false,
        30_000,
        None,
    );

    let mut processor =
        StreamProcessor::with_kafka(config, TestFactory {}, Topic::new("test_static"), None);

    for _ in 0..20 {
        processor.run_once().unwrap();
    }
}
