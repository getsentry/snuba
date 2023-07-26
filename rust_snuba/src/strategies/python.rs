use std::time::Duration;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};

use anyhow::Error;

use pyo3::prelude::*;

use crate::types::BytesInsertBatch;

use crate::config::MessageProcessorConfig;

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    py_process_message: Py<PyAny>,
    message_carried_over: Option<Message<BytesInsertBatch>>,
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
            message_carried_over: None,
        })
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        if let Some(message) = self.message_carried_over.take() {
            if let Err(MessageRejected{message: transformed_message}) = self.next_step.submit(message) {
                self.message_carried_over = Some(transformed_message);
            }
        }

        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected<KafkaPayload>> {
        if self.message_carried_over.is_some() {
            return Err(MessageRejected {
                message,
            });
        }

        // TODO: add procspawn/parallelism
        log::debug!("processing message,  message={}", message);

        let result = Python::with_gil(|py| -> PyResult<BytesInsertBatch> {
            match &message.inner_message {
                InnerMessage::AnyMessage(..) => {
                    panic!("AnyMessage cannot be processed");
                }
                InnerMessage::BrokerMessage(BrokerMessage {
                    payload,
                    offset,
                    partition,
                    timestamp,
                }) => {
                    let args = (
                        payload.payload.clone(),
                        *offset,
                        partition.index,
                        *timestamp,
                    );
                    let result = self.py_process_message.call1(py, args)?;
                    let result_decoded: Vec<Vec<u8>> = result.extract(py)?;
                    Ok(BytesInsertBatch {
                        rows: result_decoded,
                    })
                }
            }
        });

        match result {
            Ok(data) => {
                if let Err(MessageRejected{message: transformed_message}) = self.next_step.submit(message.replace(data)) {
                    self.message_carried_over = Some(transformed_message);
                }
            },
            Err(e) => {
                log::error!("Invalid message {:?}", e);
            },
        }

        Ok(())

    }

    fn close(&mut self) {
        self.next_step.close();
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        // TODO: we need to shut down the python module properly in order to avoid dataloss in
        // sentry sdk or similar things that run in python's atexit
        self.next_step.join(timeout)
    }
}
