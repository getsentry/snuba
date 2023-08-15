extern crate rust_arroyo;

use clap::{App, Arg};
use futures::executor::block_on;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::Producer;
use rust_arroyo::types::{Topic, TopicOrPartition};

async fn recreate_topic(brokers: &str, topic: &str) {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers".to_string(), brokers);

    let admin_client: AdminClient<DefaultClientContext> = config.create().unwrap();

    admin_client
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .unwrap();

    let topics = [NewTopic::new(topic, 1, TopicReplication::Fixed(1))];
    admin_client
        .create_topics(&topics, &AdminOptions::new())
        .await
        .unwrap();
}

fn generate_metric() -> String {
    r#"{"org_id": 1, "project_id": 1, "metric_id": 1000, "timestamp:: 1656183801, "tags": {"some_tag": "some_tag_value"}}".to_string()"#.to_string()
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("127.0.0.1:9092"),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .help("Kafka topic")
                .takes_value(true)
                .default_value("test_source"),
        )
        .arg(
            Arg::with_name("number")
                .long("number")
                .help("number of events to generate")
                .default_value("1000")
                .takes_value(true),
        )
        .get_matches();

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let number = matches.value_of("number").unwrap().parse::<u32>().unwrap();

    // Deletes and recreates topic
    block_on(recreate_topic(brokers, topic));

    // Create producer
    let topic = Topic {
        name: topic.to_string(),
    };
    let destination = TopicOrPartition::Topic(topic);
    let config = KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

    let mut producer = KafkaProducer::new(config);

    for _ in 0..number {
        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some(generate_metric().as_bytes().to_vec()),
        };
        producer.produce(&destination, &payload);
        producer.poll();
    }

    producer.flush();

    producer.close();
}
