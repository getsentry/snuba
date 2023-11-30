extern crate rust_arroyo;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::processing::{Callbacks, ConsumerState, StreamProcessor};
use rust_arroyo::types::Topic;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct TestFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for TestFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(CommitOffsets::new(Duration::from_secs(1)))
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let config = KafkaConfig::new_consumer_config(
        vec!["127.0.0.1:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );

    let consumer_state = Arc::new(Mutex::new(ConsumerState::new(Box::new(TestFactory {}))));

    let topic = Topic::new("test_static");
    let consumer =
        Box::new(KafkaConsumer::new(config, &[topic], Callbacks(consumer_state.clone())).unwrap());

    let mut processor = StreamProcessor::new(consumer, consumer_state, None);
    for _ in 0..20 {
        processor.run_once().unwrap();
    }
}
