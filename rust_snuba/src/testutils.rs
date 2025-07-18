use crate::types::BytesInsertBatch;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::backends::ProducerError;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, TopicOrPartition};
use std::ffi::CString;
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
        let noop_fn = py
            .eval(&CString::new("lambda *a, **kw: None").unwrap(), None, None)
            .unwrap();
        PyModule::import(py, "signal")?.setattr("signal", noop_fn)?;
        Ok(())
    })
    .unwrap();
}

pub struct TestStrategy<R> {
    pub payloads: Vec<BytesInsertBatch<R>>,
}

impl<R: Send + Sync> TestStrategy<R> {
    pub fn new() -> Self {
        Self { payloads: vec![] }
    }
}
impl<R: Send + Sync> ProcessingStrategy<BytesInsertBatch<R>> for TestStrategy<R> {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<R>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<R>>> {
        self.payloads.push(message.into_payload());
        Ok(())
    }
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
