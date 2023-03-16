use std::collections::HashMap;
use std::time::Duration;

use log;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Topic};

use pyo3::prelude::*;

use crate::settings;
use crate::storages::{self, ProcessorConfig};
use crate::strategies::noop::Noop;
use crate::strategies::python::PythonTransformStep;
use crate::types::BytesInsertBatch;

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
impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<BytesInsertBatch>) -> Result<(), MessageRejected> {
        for row in message.payload.rows {
            let decoded_row = String::from_utf8_lossy(&row);
            println!("insert: {:?}", decoded_row);
        }

        self.next_step.submit(Message {
            partition: message.partition,
            offset: message.offset,
            payload: (),
            timestamp: message.timestamp,
        })
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.next_step.join(timeout)
    }
}

#[pyfunction]
pub fn consumer(py: Python<'_>, storage: &str, settings_path: &str) {
    py.allow_threads(|| consumer_impl(storage, settings_path));
}

fn consumer_impl(storage: &str, settings_path: &str) {
    env_logger::init();
    log::info!(
        "Starting consumer for {} with settings at {}",
        storage,
        settings_path,
    );
    let settings = settings::Settings::load_from_json(settings_path).unwrap();
    log::info!("Loaded settings: {settings:?}");

    let storage_registry = storages::StorageRegistry::load_all(&settings).unwrap();
    let storage = storage_registry.get(storage).unwrap();

    struct ConsumerStrategyFactory {
        processor_config: ProcessorConfig,
    }

    impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let transform_step = PythonTransformStep::new(
                self.processor_config.clone(),
                ClickhouseWriterStep::new(Noop),
            )
            .unwrap();
            Box::new(transform_step)
        }
    }

    let mut broker_config: HashMap<_, _> = settings
        .get_broker_config(&storage.stream_loader.default_topic)
        .iter()
        .filter_map(|(k, v)| {
            let v = v.as_ref()?;
            if v.is_empty() {
                return None;
            }
            Some((k.to_owned(), v.to_owned()))
        })
        .collect();

    // TODO remove
    let consumer_group = "snuba-consumers".to_owned();
    broker_config.insert("group.id".to_owned(), consumer_group);
    let logical_topic = &storage.stream_loader.default_topic;

    let config = KafkaConfig::new_config_from_raw(broker_config);
    let processor_config = storage.stream_loader.processor.clone();

    let consumer = Box::new(KafkaConsumer::new(config));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory { processor_config }),
    );

    processor.subscribe(Topic {
        name: settings
            .kafka_topic_map
            .get(logical_topic)
            .unwrap_or(logical_topic)
            .to_owned(),
    });

    processor.run().unwrap();
}
