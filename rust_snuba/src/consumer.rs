use std::time::Duration;
use std::collections::HashMap;

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
use pyo3::types::PyDict;

use crate::settings;
use crate::storages::{self, ProcessorConfig};

use anyhow::Error;

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
    py_process_message: Py<PyAny>,
}

impl PythonTransformStep {
    fn new<N>(processor_config: ProcessorConfig, next_step: N) -> Result<Self, Error>
        where N: ProcessingStrategy<ClickhouseInsert> + 'static
    {
        let next_step = Box::new(next_step);
        let name = &processor_config.name;
        let args = serde_json::to_string(&processor_config.args).unwrap();
        let code = format!(r#"
import rapidjson
from snuba.datasets.processors import DatasetMessageProcessor

processor = DatasetMessageProcessor \
    .get_from_name("{name}") \
    .from_kwargs(**rapidjson.loads("""{args}"""))

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch

def _wrapped(message, offset, partition, timestamp):
    rv = processor.process_message(
        message=rapidjson.loads(bytearray(message)),
        metadata=KafkaMessageMetadata(
            offset=offset,
            partition=partition,
            timestamp=timestamp
        )
    )

    assert rv is None or isinstance(rv, InsertBatch), "this consumer does not support replacements"
    return rv
"#);

        let py_process_message = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let fun: Py<PyAny> = PyModule::from_code(
                py,
                &code,
                "",
                ""
            )?.getattr("_wrapped")?.into();
            Ok(fun)
        })?;

        Ok(PythonTransformStep { processor_config, next_step, py_process_message })
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        log::info!("processing message, timestamp={:?}", message.timestamp);
        let result = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let args = (message.payload.payload, message.offset, message.partition.index, message.timestamp);
            let result = self.py_process_message.call1(py, args)?;
            Ok(result)
        }).unwrap();

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

#[pyfunction]
pub fn consumer(py: Python<'_>, storage: &str, settings_path: &str) {
    py.allow_threads(|| {
        consumer_impl(storage, settings_path)
    });
}

fn consumer_impl(storage: &str, settings_path: &str) {
    env_logger::init();
    log::info!(
        "Starting consumer for {} with settings at {}",
        storage,
        settings_path,
    );
    let settings = settings::Settings::load_from_json(&settings_path).unwrap();
    log::info!("Loaded settings: {settings:?}");

    let storage_registry = storages::StorageRegistry::load_all(&settings).unwrap();
    let storage = storage_registry.get(&storage).unwrap();

    struct ConsumerStrategyFactory {
        config: KafkaConfig,
        processor_config: ProcessorConfig,
    }

    impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let transform_step = PythonTransformStep::new(
                self.processor_config.clone(),
                Noop {}
            ).unwrap();
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
    let consumer_group = "snuba-consumers".to_owned();
    broker_config.insert("group.id".to_owned(), consumer_group.clone());
    let logical_topic = &storage.stream_loader.default_topic;

    let config = KafkaConfig::new_config_from_raw(broker_config);
    let processor_config = storage.stream_loader.processor.clone();

    let consumer = Box::new(KafkaConsumer::new(config.clone()));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory {
            config,
            processor_config,
        })
    );

    processor.subscribe(Topic {
        name: settings.kafka_topic_map.get(logical_topic).unwrap_or(logical_topic).to_owned(),
    });

    processor.run().unwrap();
}
