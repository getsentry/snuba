use crate::types::BytesInsertBatch;
use pyo3::prelude::*;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::Producer;
use rust_arroyo::backends::ProducerError;
use rust_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use rust_arroyo::types::{Message, TopicOrPartition};
use std::str;
use std::sync::{Arc, Mutex};

use std::time::Duration;

pub fn initialize_python() {
    let python_executable = std::env::var("SNUBA_TEST_PYTHONEXECUTABLE").unwrap();
    let python_path = std::env::var("SNUBA_TEST_PYTHONPATH").unwrap();
    let python_path: Vec<_> = python_path.split(':').map(String::from).collect();

    Python::with_gil(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", python_executable)?;
        PyModule::import(py, "sys")?.setattr("path", python_path)?;

        // monkeypatch signal handlers in Python to noop, because otherwise python multiprocessing
        // strategies cannot be tested
        let noop_fn = py.eval("lambda *a, **kw: None", None, None).unwrap();
        PyModule::import(py, "signal")?.setattr("signal", noop_fn)?;
        Ok(())
    })
    .unwrap();
}

pub struct TestStrategy {
    pub payloads: Vec<BytesInsertBatch>,
}

impl TestStrategy {
    pub fn new() -> Self {
        Self { payloads: vec![] }
    }
}
impl ProcessingStrategy<BytesInsertBatch> for TestStrategy {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
    fn submit(
        &mut self,
        message: Message<BytesInsertBatch>,
    ) -> Result<(), SubmitError<BytesInsertBatch>> {
        self.payloads.push(message.payload().clone());
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
}

pub struct MockProducer {
    pub payloads: Arc<Mutex<Vec<(String, KafkaPayload)>>>,
}

impl Producer<KafkaPayload> for MockProducer {
    fn produce(
        &self,
        _topic: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        self.payloads.lock().unwrap().push((
            str::from_utf8(payload.key().unwrap()).unwrap().to_owned(),
            payload,
        ));
        Ok(())
    }
}
