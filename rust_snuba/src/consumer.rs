use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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

use crate::strategies::python::PythonTransformStep;
use crate::types::BytesInsertBatch;
use crate::{config, setup_sentry};

struct ClickhouseWriterStep {
    next_step: Box<dyn ProcessingStrategy<()>>,
}

impl ClickhouseWriterStep {
    fn new<N>(next_step: N) -> Self
    where
        N: ProcessingStrategy<()> + 'static,
    {
        ClickhouseWriterStep {
            next_step: Box::new(next_step),
        }
    }
}
impl ProcessingStrategy<Vec<BytesInsertBatch>> for ClickhouseWriterStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<Vec<BytesInsertBatch>>) -> Result<(), MessageRejected> {
        for batch in message.payload() {
            for row in batch.rows {
                let decoded_row = String::from_utf8_lossy(&row);
                log::debug!("insert: {:?}", decoded_row);
            }
        }

        log::info!("Insert {} rows", message.payload().len());

        self.next_step.submit(message.replace(()))
    }

    fn close(&mut self) {
        self.next_step.close();
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
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
    struct ConsumerStrategyFactory {
        processor_config: config::MessageProcessorConfig,
        max_batch_size: usize,
        max_batch_time: Duration,
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
                    Box::new(ClickhouseWriterStep::new(CommitOffsets::new(
                        Duration::from_secs(1),
                    ))),
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

    let consumer = Box::new(KafkaConsumer::new(config));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory {
            processor_config,
            max_batch_size,
            max_batch_time,
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
