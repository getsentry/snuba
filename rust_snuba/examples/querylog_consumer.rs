extern crate rust_snuba;

use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rust_snuba::backends::Consumer as ConsumerTrait;
use rust_snuba::backends::kafka::CustomContext;
use rust_snuba::backends::kafka::config::KafkaConfig;
use rust_snuba::backends::kafka::KafkaConsumer;
use rust_snuba::backends::AssignmentCallbacks;
use rust_snuba::processing::querylog_processor;
use rust_snuba::processing::querylog_processor::ProcessedQueryLog;
use rust_snuba::processing::querylog_processor::RawQueryLogKafkaJson;
use rust_snuba::types::{Partition, Topic};
use std::collections::HashMap;
use std::time::Duration;
use rdkafka::consumer::base_consumer::BaseConsumer;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

fn commit_offsets(
    consumer: &mut StreamConsumer<CustomContext>,
    topic: String,
    offsets_to_commit: HashMap<i32, u64>,
) {
    let topic_map = offsets_to_commit
        .iter()
        .map(|(k, v)| ((topic.clone(), *k), Offset::from_raw((*v + 1) as i64)))
        .collect();
    let partition_list = TopicPartitionList::from_topic_map(&topic_map).unwrap();
    consumer.commit(&partition_list, CommitMode::Sync).unwrap();


    // .commit(&partition_list, CommitMode::Sync).unwrap();
}

fn main() {
    let config = KafkaConfig::new_consumer_config(
        vec!["localhost:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );
    let mut consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: "snuba-queries".to_string(),
    };
    let res = consumer.subscribe(&[topic], Box::new(EmptyCallbacks {}));
    assert!(res.is_ok());
    println!("Subscribed");

    // infinite loop
    loop {
        println!("Polling");
        let res = consumer.poll(Some(Duration::from_millis(2000)));
        match res.unwrap() {
            Some(x) => {
                println!("MSG {}", x);
                // TODO: do something with the message
                let payload = x.payload.payload.unwrap();

                // send msg x to clickhouse using Clickhouse Client
                let json_str = String::from_utf8(payload).unwrap();
                let json_obj: RawQueryLogKafkaJson = serde_json::from_str(&json_str);
                match json_obj {
                    Ok(json_obj) => {
                        println!("json_obj: {}", json_obj);
                        let mut processed_result: ProcessedQueryLog = {};
                        querylog_processor::process(json_obj,  &mut &processed_result);
                        println!("processed_result: {}", processed_result);
                    }
                    Err(e) => {
                        println!("JSON Error: {}", e);
                        fatal!("JSON Error: {}", e);
                    }
                }




            }
            None => {}
            // todo!(),
        }
    }
}
