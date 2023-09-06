use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::InvalidMessage;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message, Topic};

use pyo3::prelude::*;

use crate::processors;
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::python::PythonTransformStep;
use crate::types::{BytesInsertBatch, KafkaMessageMetadata};
use crate::{config, setup_sentry};

#[pyfunction]
pub fn consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    consumer_config_raw: &str,
    skip_write: bool,
    processes: usize,
    use_rust_processor: bool,
) {
    py.allow_threads(|| {
        consumer_impl(
            consumer_group,
            auto_offset_reset,
            consumer_config_raw,
            skip_write,
            processes,
            use_rust_processor,
        )
    });
}

pub fn consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    consumer_config_raw: &str,
    skip_write: bool,
    processes: usize,
    use_rust_processor: bool,
) {
    struct ConsumerStrategyFactory {
        processor_config: config::MessageProcessorConfig,
        max_batch_size: usize,
        max_batch_time: Duration,
        clickhouse_cluster_config: config::ClickhouseConfig,
        clickhouse_table_name: String,
        skip_write: bool,
        processes: usize,
        use_rust_processor: bool,
    }

    impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let accumulator = Arc::new(|mut acc: BytesInsertBatch, value: BytesInsertBatch| {
                for row in value.rows {
                    acc.rows.push(row);
                }
                acc
            });

            let next_step = Reduce::new(
                Box::new(ClickhouseWriterStep::new(
                    CommitOffsets::new(Duration::from_secs(1)),
                    self.clickhouse_cluster_config.clone(),
                    self.clickhouse_table_name.clone(),
                    self.skip_write,
                    2,
                )),
                accumulator,
                BytesInsertBatch { rows: vec![] },
                self.max_batch_size,
                self.max_batch_time,
            );

            match (
                self.use_rust_processor,
                processors::get_processing_function(&self.processor_config.python_class_name),
            ) {
                (true, Some(func)) => {
                    struct MessageProcessor {
                        func: fn(
                            KafkaPayload,
                            KafkaMessageMetadata,
                        )
                            -> Result<BytesInsertBatch, InvalidMessage>,
                    }

                    impl TaskRunner<KafkaPayload, BytesInsertBatch> for MessageProcessor {
                        fn get_task(
                            &self,
                            message: Message<KafkaPayload>,
                        ) -> RunTaskFunc<BytesInsertBatch> {
                            let func = self.func;

                            Box::pin(async move {
                                let broker_message = match message.inner_message {
                                    InnerMessage::BrokerMessage(msg) => msg,
                                    _ => panic!("Unexpected message type"),
                                };

                                let metadata = KafkaMessageMetadata {
                                    partition: broker_message.partition.index,
                                    offset: broker_message.offset,
                                    timestamp: broker_message.timestamp,
                                };

                                match func(broker_message.payload, metadata) {
                                    Ok(transformed) => Ok(Message {
                                        inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                                            payload: transformed,
                                            partition: broker_message.partition,
                                            offset: broker_message.offset,
                                            timestamp: broker_message.timestamp,
                                        }),
                                    }),
                                    Err(e) => Err(e),
                                }
                            })
                        }
                    }

                    let task_runner = MessageProcessor { func };
                    Box::new(RunTaskInThreads::new(next_step, Box::new(task_runner), 100))
                }
                _ => Box::new(
                    PythonTransformStep::new(
                        self.processor_config.clone(),
                        self.processes,
                        next_step,
                    )
                    .unwrap(),
                ),
            }
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

    procspawn::init();

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
    let clickhouse_cluster_config = first_storage.clickhouse_cluster.clone();
    let clickhouse_table_name = first_storage.clickhouse_table_name.clone();

    let consumer = Box::new(KafkaConsumer::new(config));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory {
            processor_config,
            max_batch_size,
            max_batch_time,
            clickhouse_cluster_config,
            clickhouse_table_name,
            skip_write,
            processes,
            use_rust_processor,
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

    processor.run().unwrap();
}

#[pyfunction]
pub fn process_message(
    name: &str,
    value: Vec<u8>,
    partition: u16,
    offset: u64,
    millis_since_epoch: i64,
) -> Option<Vec<u8>> {
    // XXX: Currently only takes the message payload and metadata. This assumes
    // key and headers are not used for message processing
    match processors::get_processing_function(name) {
        None => None,
        Some(func) => {
            let payload = KafkaPayload {
                key: None,
                headers: None,
                payload: Some(value),
            };

            let meta = KafkaMessageMetadata {
                partition,
                offset,
                timestamp: DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::from_timestamp_millis(millis_since_epoch)
                        .unwrap_or(NaiveDateTime::MIN),
                    Utc,
                ),
            };

            let res = func(payload, meta);
            println!("res {:?}", res);
            let row = res.unwrap().rows[0].clone();
            Some(row)
        }
    }
}
