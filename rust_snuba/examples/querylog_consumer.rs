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
use rust_snuba::utils::clickhouse_client;
use rust_snuba::utils::clickhouse_client::ClickhouseClient;
use std::collections::HashMap;
use std::time::Duration;
use rdkafka::consumer::base_consumer::BaseConsumer;


const TIMEOUT_MS: Duration = Duration::from_millis(2000);
const TABLE_NAME: &str = "querylog";

pub struct QueryLogConsumer{
    consumer: KafkaConsumer,
    topic: Topic,
    config: KafkaConfig,
    client: ClickhouseClient,
}

impl QueryLogConsumer {
    pub fn run(){

    }
}

pub fn new(config: KafkaConfig, topic: Topic, client: ClickhouseClient,
    clickhouse_hostname: String, clickhouse_port: u16, clickhouse_table: String) -> QueryLogConsumer {
    let consumer = KafkaConsumer::new(config.clone());
    let client: ClickhouseClient = clickhouse_client::new(clickhouse_hostname,clickhouse_port, clickhouse_table);
    QueryLogConsumer {
        consumer,
        topic,
        config,
        client
    }
}


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
                println!("\n MSG {}", x);
                // TODO: do something with the message
                let payload = x.payload.payload.unwrap();

                // send msg x to clickhouse using Clickhouse Client
                let json_str = String::from_utf8(payload).unwrap();
                let some_json_obj: Result<RawQueryLogKafkaJson, serde_json::Error> = serde_json::from_str(&json_str);
                match some_json_obj {
                    Ok(json_obj) => {
                        println!("\n json_obj: {}", serde_json::to_string(&json_obj).unwrap());
                        let processed_result: &mut ProcessedQueryLog =  &mut ProcessedQueryLog::default();
                        querylog_processor::process(json_obj,  processed_result);
                        println!("\n processed_result: {:?}", processed_result);
                    }
                    Err(e) => {
                        println!("\nJSON Error: {}", e);
                        panic!("JSON Error: {}", e)
                    }
                }




            }
            None => {}
            // todo!(),
        }
    }
}
