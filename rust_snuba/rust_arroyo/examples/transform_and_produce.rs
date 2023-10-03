// An example of using the Transform and Produce strategies together.
// inspired by https://github.com/getsentry/arroyo/blob/main/examples/transform_and_produce/script.py
// This creates a consumer that reads from a topic test_in, reverses the string message,
// and then produces it to topic test_out.
extern crate rust_arroyo;

use rdkafka::message::ToBytes;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::produce::Produce;
use rust_arroyo::processing::strategies::transform::Transform;
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Topic, TopicOrPartition};
use std::time::Duration;

fn reverse_string(value: KafkaPayload) -> Result<KafkaPayload, InvalidMessage> {
    let payload = value.payload.unwrap();
    let str_payload = std::str::from_utf8(&payload).unwrap();
    let result_str = str_payload.chars().rev().collect::<String>();

    println!("transforming value: {:?} -> {:?}", str_payload, &result_str);

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
    fn submit(
        &mut self,
        _message: Message<KafkaPayload>,
    ) -> Result<(), MessageRejected<KafkaPayload>> {
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}

#[tokio::main]
async fn main() {
    struct ReverseStringAndProduceStrategyFactory {
        config: KafkaConfig,
        topic: Topic,
    }
    impl ProcessingStrategyFactory<KafkaPayload> for ReverseStringAndProduceStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let producer = KafkaProducer::new(self.config.clone());
            let topic = TopicOrPartition::Topic(self.topic.clone());
            let reverse_string_and_produce_strategy =
                Transform::new(reverse_string, Produce::new(Noop {}, producer, 5, topic));
            Box::new(reverse_string_and_produce_strategy)
        }
    }

    let config = KafkaConfig::new_consumer_config(
        vec!["0.0.0.0:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );

    let consumer = Box::new(KafkaConsumer::new(config.clone()));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ReverseStringAndProduceStrategyFactory {
            config: config.clone(),
            topic: Topic {
                name: "test_out".to_string(),
            },
        }),
    );
    processor.subscribe(Topic {
        name: "test_in".to_string(),
    });
    println!("running processor. transforming from test_in to test_out");
    processor.run().unwrap();
}
