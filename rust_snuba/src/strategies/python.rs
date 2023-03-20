use std::time::Duration;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::Message;

use anyhow::Error;

use pyo3::prelude::*;

use crate::types::BytesInsertBatch;

use crate::config::MessageProcessorConfig;

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    py_process_message: Py<PyAny>,
}

impl PythonTransformStep {
    pub fn new<N>(processor_config: MessageProcessorConfig, next_step: N) -> Result<Self, Error>
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let next_step = Box::new(next_step);
        let python_module = &processor_config.python_module;
        let python_class_name = &processor_config.python_class_name;
        let code = format!(
            r#"
import rapidjson
from snuba.datasets.processors import DatasetMessageProcessor

from {python_module} import {python_class_name} as Processor

processor = Processor.from_kwargs()

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch
from datetime import datetime
from snuba.consumers.consumer import json_row_encoder


def _wrapped(message, offset, partition, timestamp):
    rv = processor.process_message(
        message=rapidjson.loads(bytearray(message)),
        metadata=KafkaMessageMetadata(
            offset=offset,
            partition=partition,
            timestamp=timestamp
        )
    )

    if rv is None:
        return rv

    assert isinstance(rv, InsertBatch), "this consumer does not support replacements"

    return [json_row_encoder.encode(row) for row in rv.rows]
"#
        );

        let py_process_message = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let fun: Py<PyAny> = PyModule::from_code(py, &code, "", "")?
                .getattr("_wrapped")?
                .into();
            Ok(fun)
        })?;

        Ok(PythonTransformStep {
            next_step,
            py_process_message,
        })
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        // TODO: add procspawn/parallelism
        log::debug!("processing message, timestamp={:?}", message.timestamp);
        let result = Python::with_gil(|py| -> PyResult<BytesInsertBatch> {
            let args = (
                message.payload.payload,
                message.offset,
                message.partition.index,
                message.timestamp,
            );
            let result = self.py_process_message.call1(py, args)?;
            let result_decoded: Vec<Vec<u8>> = result.extract(py)?;
            Ok(BytesInsertBatch {
                rows: result_decoded,
            })
        })
        .unwrap();

        self.next_step.submit(Message {
            partition: message.partition,
            offset: message.offset,
            payload: result,
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
        // TODO: we need to shut down the python module properly in order to avoid dataloss in
        // sentry sdk or similar things that run in python's atexit
        self.next_step.join(timeout)
    }
}
