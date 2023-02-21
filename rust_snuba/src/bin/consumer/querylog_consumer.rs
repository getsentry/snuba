use rust_snuba::consumer::querylog_consumer::QueryLogConsumer;
use rust_arroyo::{backends::kafka::config::KafkaConfig, types::Topic, utils::clickhouse_client::ClickhouseClient};

const TABLE_NAME: &str = "default.querylog_local";

// looks like the reqwest client needs tokio runtime
#[tokio::main]
async fn main() {

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
    let client: ClickhouseClient = ClickhouseClient::new("localhost",8123, TABLE_NAME);

    let mut querylog_consumer: QueryLogConsumer = QueryLogConsumer::new(config, topic, client);
    querylog_consumer.run().await;
}
