// extern crate rust_snuba;

use futures::executor::block_on;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use std::collections::HashMap;
use std::time::Duration;
use rdkafka::consumer::base_consumer::BaseConsumer;

use crate::backends::AssignmentCallbacks;
use crate::backends::Consumer as ConsumerTrait;
use crate::backends::kafka::CustomContext;
use crate::backends::kafka::KafkaConsumer;
use crate::backends::kafka::config::KafkaConfig;
use crate::processing::querylog_processor;
use crate::processing::querylog_processor::ProcessedQueryLog;
use crate::processing::querylog_processor::RawQueryLogKafkaJson;
use crate::types::Partition;
use crate::types::Topic;
use crate::utils::clickhouse_client;
use crate::utils::clickhouse_client::ClickhouseClient;


const TIMEOUT_MS: Duration = Duration::from_millis(2000);
const TABLE_NAME: &str = "default.querylog_local";

pub struct QueryLogConsumer{
    consumer: KafkaConsumer,
    topic: Topic,
    config: KafkaConfig,
    client: ClickhouseClient,
}



impl QueryLogConsumer {
    pub fn new(config: KafkaConfig, topic: Topic, client: ClickhouseClient) -> QueryLogConsumer {
        let mut consumer = KafkaConsumer::new(config.clone());
        let res = consumer.subscribe(&[topic.clone()], Box::new(EmptyCallbacks {}));
        assert!(res.is_ok());
        println!("Subscribed");

        QueryLogConsumer {
            consumer,
            topic,
            config,
            client
        }
    }

    pub async fn run(&mut self){
        // infinite loop
        loop {
            println!("Polling");
            let res = self.consumer.poll(Some(TIMEOUT_MS));
            match res.unwrap() {
                Some(kafka_msg) => {
                    println!("\n MSG {}", kafka_msg);
                    // TODO: do something with the message
                    let payload = kafka_msg.payload.payload.unwrap();
                    let json_str = String::from_utf8(payload).unwrap();
                    let some_json_obj: Result<RawQueryLogKafkaJson, serde_json::Error> = serde_json::from_str(&json_str);
                    match some_json_obj {
                        Ok(json_obj) => {
                            println!("\n json_obj: {}", serde_json::to_string(&json_obj).unwrap());
                            let processed_result: &mut ProcessedQueryLog =  &mut ProcessedQueryLog::default();
                            querylog_processor::process(json_obj,  processed_result);
                            println!("\n processed_result: {:?}", processed_result);

                            // send msg x to clickhouse using Clickhouse Client
                            let body = serde_json::to_string(&processed_result).unwrap();
                            self.client.send(body).await.unwrap();

                            // commit offset
                            self.consumer.commit_positions().unwrap();
                        }
                        Err(e) => {
                            println!("\nJSON Error: {}", e);
                            panic!("JSON Error: {}", e)
                        }
                    }
                }
                None => {}
            }
        }
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

    // TODO we can get these from the yaml file
    let config = KafkaConfig::new_consumer_config(
        vec!["localhost:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );
    // let mut consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: "snuba-queries".to_string(),
    };
    let client: ClickhouseClient = clickhouse_client::new("localhost",9000, TABLE_NAME);

    let mut querylog_consumer: QueryLogConsumer = QueryLogConsumer::new(config, topic, client);
    block_on(querylog_consumer.run());

}
