use std::time::Duration;
use std::collections::HashMap;

use clap::Parser;
use log;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::transform::Transform;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory, InvalidMessage,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Topic, TopicOrPartition};

use pyo3::prelude::*;

use rust_snuba::settings;
use rust_snuba::storages::{self, ProcessorConfig};

use anyhow::Error;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    storage: String,

    #[arg(long)]
    settings_path: String,
}

struct Noop {}
impl<T> ProcessingStrategy<T> for Noop where T: Clone {
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }
    fn submit(&mut self, _message: Message<T>) -> Result<(), MessageRejected> {
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}

#[derive(Clone)]
struct ClickhouseInsert {}

struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<ClickhouseInsert>>,
    processor_config: ProcessorConfig,
    py_processor: Py<PyAny>,
}

impl PythonTransformStep {
    fn new<N>(processor_config: ProcessorConfig, next_step: N) -> Result<Self, Error>
        where N: ProcessingStrategy<ClickhouseInsert> + 'static
    {
        let next_step = Box::new(next_step);
        let name = &processor_config.name;
        let args = serde_json::to_string(&processor_config.args).unwrap();
        let code = format!(r#"
import sys
import os
sys.path.extend(os.environ["SNUBA_PYTHONPATH"].split(":"))

import json
from snuba.datasets.processors import DatasetMessageProcessor

processor = DatasetMessageProcessor \
    .get_from_name("{name}") \
    .from_kwargs(**json.loads("""{args}"""))"#);

        let py_processor = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let fun: Py<PyAny> = PyModule::from_code(
                py,
                &code,
                "",
                ""
            )?.getattr("processor")?.into();
            Ok(fun)
        })?;

        Ok(PythonTransformStep { processor_config, next_step, py_processor })
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        log::info!("processing message");
        let transformed = ClickhouseInsert {};

        self.next_step.submit(Message {
            partition: message.partition,
            offset: message.offset,
            payload: transformed,
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

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    log::info!(
        "Starting consumer for {} with settings at {}",
        args.storage,
        args.settings_path,
    );
    let settings = settings::Settings::load_from_json(&args.settings_path).unwrap();
    log::info!("Loaded settings: {settings:?}");

    let storage_registry = storages::StorageRegistry::load_all(&settings).unwrap();
    let storage = storage_registry.get(&args.storage).unwrap();

    struct ConsumerStrategyFactory {
        config: KafkaConfig,
        processor_config: ProcessorConfig,
    }

    impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let transform_step = PythonTransformStep::new(self.processor_config.clone(), Noop {}).unwrap();
            Box::new(transform_step)
        }
    }

    let mut broker_config: HashMap<_, _> = settings.get_broker_config(&storage.stream_loader.default_topic)
        .iter()
        .filter_map(|(k, v)| {
            let v = v.as_ref()?;
            if v.is_empty() { return None }
            Some((k.to_owned(), v.to_owned()))
        })
        .collect();

    // TODO remove
    broker_config.insert("group.id".to_owned(), "snuba-consumers".to_owned());

    let config = KafkaConfig::new_config_from_raw(broker_config);
    let processor_config = storage.stream_loader.processor.clone();

    let consumer = Box::new(KafkaConsumer::new(config.clone()));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory { config, processor_config })
    );

    processor.subscribe(Topic {
        name: settings.kafka_topic_map.get(&storage.stream_loader.default_topic).unwrap_or(&storage.stream_loader.default_topic).to_owned(),
    });

    processor.run().unwrap();
}
