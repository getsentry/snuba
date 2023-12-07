extern crate rust_arroyo;

use rust_arroyo::backends::kafka::config::KafkaConsumerConfig;
use rust_arroyo::backends::kafka::InitialOffset;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::backends::CommitOffsets;
use rust_arroyo::backends::Consumer;
use rust_arroyo::types::{Partition, Topic};
use std::collections::HashMap;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&self, _: HashMap<Partition, u64>) {}
    fn on_revoke<C: CommitOffsets>(&self, _: C, _: Vec<Partition>) {}
}

fn main() {
    tracing_subscriber::fmt::init();

    let config = KafkaConsumerConfig::new(
        vec!["127.0.0.1:9092".to_string()],
        "my_group".to_string(),
        InitialOffset::Latest,
        false,
        30_000,
        None,
    );

    let topic = Topic::new("test_static");
    let mut consumer = KafkaConsumer::new(config, &[topic], EmptyCallbacks {}).unwrap();
    println!("Subscribed");
    for _ in 0..20 {
        println!("Polling");
        let res = consumer.poll(None);
        if let Some(x) = res.unwrap() {
            println!("MSG {:?}", x)
        }
    }
}
