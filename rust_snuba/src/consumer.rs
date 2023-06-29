use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Topic};

use pyo3::prelude::*;
use rust_arroyo::utils::clickhouse_client::ClickhouseClient;

use crate::config::ClickhouseConfig;
use crate::strategies::python::PythonTransformStep;
use crate::types::BytesInsertBatch;
use crate::{config, setup_sentry};

struct ClickhouseWriterStep {
    next_step: Box<dyn ProcessingStrategy<()>>,
    clickhouse_client: ClickhouseClient,
}

impl ClickhouseWriterStep {
    fn new<N>(next_step: N, cluster_config: ClickhouseConfig, table: String) -> Self
    where
        N: ProcessingStrategy<()> + 'static,
    {
        let hostname = cluster_config.host;
        let http_port = cluster_config.http_port;
        let database = cluster_config.database;

        ClickhouseWriterStep {
            next_step: Box::new(next_step),
            clickhouse_client: ClickhouseClient::new(&hostname, http_port, &table, Some(&database)),
        }
    }
}

#[async_trait]
impl ProcessingStrategy<Vec<BytesInsertBatch>> for ClickhouseWriterStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        log::trace!("polling clickhouse writer step");
        self.next_step.poll()
    }

    async fn submit(&mut self, message: Message<Vec<BytesInsertBatch>>) -> Result<(), MessageRejected> {
        log::trace!("submitting clickhouse writer step");
        for batch in message.payload() {
            for row in batch.rows {
                let decoded_row = String::from_utf8_lossy(&row);
                let res = self.clickhouse_client.send( decoded_row.to_string()).await;
                match res {
                    Ok(r) => {
                        match r.error_for_status() {
                            Ok(_) => {
                                log::trace!("insert: {:?}", decoded_row);
                            }
                            Err(e) => {
                                log::error!("Error sending to clickhouse: {}", e);
                                return Err(MessageRejected);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error sending to clickhouse: {}", e);
                        return Err(MessageRejected);
                    }
                }
            }
        }

        log::debug!("Insert {} rows", message.payload().len());
        self.next_step.submit(message.replace(())).await
    }

    fn close(&mut self) {
        log::debug!("closing clickhouse writer step");
        self.next_step.close();
    }

    fn terminate(&mut self) {
        log::debug!("terminating clickhouse writer step");
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        log::debug!("joining clickhouse writer step");
        self.next_step.join(timeout)
    }
}

#[pyfunction]
pub fn consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    consumer_config_raw: &str,
) {
    py.allow_threads(|| consumer_impl(consumer_group, auto_offset_reset, consumer_config_raw));
}

pub fn consumer_impl(consumer_group: &str, auto_offset_reset: &str, consumer_config_raw: &str) {
    // let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    struct ConsumerStrategyFactory {
        processor_config: config::MessageProcessorConfig,
        max_batch_size: usize,
        max_batch_time: Duration,
        cluster_config: ClickhouseConfig,
        table: String,
    }

    impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {

        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let accumulator =
                Arc::new(|mut acc: Vec<BytesInsertBatch>, value: BytesInsertBatch| {
                    acc.push(value);
                    acc
                });

            let transform_step = PythonTransformStep::new(
                self.processor_config.clone(),
                Reduce::new(
                    Box::new(ClickhouseWriterStep::new(
                        CommitOffsets::new(Duration::from_secs(1)),
                        self.cluster_config.clone(),
                        self.table.clone(),
                    )),
                    accumulator,
                    Vec::new(),
                    self.max_batch_size,
                    self.max_batch_time,
                ),
            )
            .unwrap();
            Box::new(transform_step)
        }
    }

    env_logger::init();
    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
    let max_batch_size = consumer_config.max_batch_size;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    // TODO: Support multiple storages
    assert_eq!(consumer_config.storages.len(), 1);
    assert!(consumer_config.replacements_topic.is_none());
    assert!(consumer_config.commit_log_topic.is_none());

    // setup sentry
    if let Some(env) = consumer_config.env {
        if let Some(dsn) = env.sentry_dsn {
            log::debug!("Using sentry dsn {:?}", dsn);
            setup_sentry(dsn);
        }
    }

    let first_storage = &consumer_config.storages[0];

    log::info!("Starting consumer for {:?}", first_storage.name,);

    let broker_config: HashMap<_, _> = consumer_config
        .raw_topic
        .broker_config
        .iter()
        .filter_map(|(k, v)| {
            let v = v.as_ref()?;
            if v.is_empty() {
                return None;
            }
            Some((k.to_owned(), v.to_owned()))
        })
        .collect();

    let config = KafkaConfig::new_consumer_config(
        vec![],
        consumer_group.to_owned(),
        auto_offset_reset.to_owned(),
        false,
        Some(broker_config),
    );

    let processor_config = first_storage.message_processor.clone();

    let clickhouse_table_name = first_storage.clickhouse_table_name.clone();
    let clickhouse_cluster = first_storage.clickhouse_cluster.clone();

    let consumer = Box::new(KafkaConsumer::new(config));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory {
            processor_config,
            max_batch_size,
            max_batch_time,
            cluster_config: clickhouse_cluster,
            table: clickhouse_table_name,
        }),
    );

    processor.subscribe(Topic {
        name: consumer_config.raw_topic.physical_topic_name.to_owned(),
    });

    let mut handle = processor.get_handle();

    ctrlc::set_handler(move || {
        handle.signal_shutdown();
    })
    .expect("Error setting Ctrl-C handler");



    tokio_rt.block_on(processor.run()).unwrap();
    log::info!("Consumer terminated");

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ClickhouseWriterStep;
    use ::chrono::Utc;
    use rust_arroyo::types::BrokerMessage;
    use rust_arroyo::types::InnerMessage;
    use rust_arroyo::types::Partition;
    use rust_arroyo::types::{Message, Topic};
    use reqwest::{Client};

    const HOSTNAME: &str = "localhost";
    const HTTP_PORT: i16 = 8123;
    const DATABASE: &str = "snuba_test";
    const TABLE: &str = "rust_test";
    const CONSUMER_CONFIG_RAW: &str = r#"
        {
            "storages": [
                {
                    "name": "test",
                    "clickhouse_table_name": "rust_test",
                    "clickhouse_cluster": {
                        "host": "localhost",
                        "port": 9000,
                        "http_port": 8123,
                        "user" : "user1",
                        "password": "pword",
                        "database": "snuba_test"
                    },
                    "message_processor": {
                        "python_class_name": "QuerylogProcessor",
                        "python_module": "snuba.datasets.processors.querylog_processor"
                    }
                }
            ],
            "raw_topic": {
                "physical_topic_name": "test",
                "logical_topic_name": "test",
                "broker_config": {
                    "bootstrap.servers": "localhost:9092"
                }
            },
            "max_batch_size": 1000,
            "max_batch_time_ms": 1000
        }
        "#;

    async fn setup_clickhouse_table() -> Result<(), reqwest::Error>{
        let client = Client::new();
        let table = format!("{}.{}", DATABASE, TABLE);
        let res = client
            .post(format!("http://{}:{}", HOSTNAME, HTTP_PORT))
            .body(format!("CREATE TABLE IF NOT EXISTS {} (name String, id UInt64) ENGINE = MergeTree ORDER BY name;", table))
            .send().await?;
        assert!(res.status().is_success(), "Failed to create table: {:?}", res.text().await?);
        Ok(())
    }

    #[test]
    fn parse_config(){
        config::ConsumerConfig::load_from_str(CONSUMER_CONFIG_RAW).unwrap();
    }

    #[ignore = "clickhouse not running in rust ci"]
    #[tokio::test]
    async fn create_clickhouse_writer() -> Result<(), reqwest::Error> {
        setup_clickhouse_table().await?;
        let consumer_config = config::ConsumerConfig::load_from_str(CONSUMER_CONFIG_RAW).unwrap();
        let first_storage = &consumer_config.storages[0];
        let clickhouse_table_name = first_storage.clickhouse_table_name.clone();
        let clickhouse_cluster = first_storage.clickhouse_cluster.clone();

        let mut clickhouse_writer_step = ClickhouseWriterStep::new(
            CommitOffsets::new(Duration::from_secs(1)),
            clickhouse_cluster,
            clickhouse_table_name,
        );

        let topic = Topic {name: "test".to_string()};
        let part = Partition { topic, index: 10 };
        let err_batch = BytesInsertBatch{
            rows: vec![r#"{'a': 123}"#.as_bytes().to_vec()]
        };
        let good_batch = BytesInsertBatch{
            rows: vec![r#"[{"name": "alice", "id": 1}]"#.as_bytes().to_vec()]
        };

        assert!(clickhouse_writer_step.submit(Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                partition: part.clone(),
                offset: 0,
                timestamp: Utc::now(),
                payload: vec![err_batch],
            })
        }).await.is_err());

        assert!(clickhouse_writer_step.submit(Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                partition: part,
                offset: 0,
                timestamp: Utc::now(),
                payload: vec![good_batch],
            })
        }).await.is_ok());
        Ok(())
    }
}
