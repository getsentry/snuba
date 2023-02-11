// extern crate rust_snuba;

use futures::executor::block_on;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use std::collections::HashMap;
use std::time::Duration;

use crate::backends::AssignmentCallbacks;
use crate::backends::Consumer as ConsumerTrait;
use crate::backends::kafka::CustomContext;
use crate::backends::kafka::KafkaConsumer;
use crate::backends::kafka::config::KafkaConfig;
use crate::processing::querylog_processor;
use crate::processing::querylog_processor::RawQueryLogKafkaJson;
use crate::types::Partition;
use crate::types::Topic;
use crate::utils::clickhouse_client::ClickhouseClient;


const TIMEOUT_MS: Duration = Duration::from_millis(2000);

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
                            let processed_json_body = querylog_processor::process(json_obj);

                            // send msg x to clickhouse using Clickhouse Client
                            println!("\n processed_result:\n {}\n", processed_json_body);
                            let resp = self.client.send(processed_json_body).await.unwrap();
                            assert!(resp.status().is_success());

                            // commit offset //TODO figure out how to handle offsets
                            // self.consumer.commit_pos itions().unwrap();
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

}
