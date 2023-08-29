use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};

use std::time::Duration;
use std::sync::Mutex;

use anyhow::Error;

use pyo3::prelude::*;

use crate::types::BytesInsertBatch;

use crate::config::MessageProcessorConfig;

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    handles: Vec<(
        Message<()>,
        Mutex<procspawn::JoinHandle<Result<BytesInsertBatch, String>>>
    )>,
    message_carried_over: Option<Message<BytesInsertBatch>>,
    processing_pool: procspawn::Pool,
}

impl PythonTransformStep {
    pub fn new<N>(processor_config: MessageProcessorConfig, processes: usize, next_step: N) -> Result<Self, Error>
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let next_step = Box::new(next_step);
        let python_module = &processor_config.python_module;
        let python_class_name = &processor_config.python_class_name;

        Ok(PythonTransformStep {
            next_step,
            handles: vec![],
            message_carried_over: None,
            processing_pool: procspawn::Pool::builder(processes)
                .env("RUST_SNUBA_PROCESSOR_MODULE", python_module)
                .env("RUST_SNUBA_PROCESSOR_CLASSNAME", python_class_name)
                .build().expect("failed to build procspawn pool"),
        })
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(
        &mut self,
        message: Message<KafkaPayload>,
    ) -> Result<(), MessageRejected<KafkaPayload>> {
        // if there are a lot of "queued" messages (=messages waiting for a free process), let's
        // not enqueue more.
        //
        // this threshold was chosen arbitrarily with no performance measuring, and we may also
        // check for queued_count() > 0 instead, but the rough idea of comparing with
        // active_count() is that we allow for one pending message per process, so that procspawn
        // is able to keep CPU saturation high.
        if self.processing_pool.queued_count() > self.processing_pool.active_count() {
            return Err(MessageRejected { message });
        }

        // procspawn has no join() timeout, but does give us the number of active tasks. if there
        // are no tasks, we can conclude that there are either no handles or all handles are done.
        //
        // it's probably not hard to add join() timeout support to procspawn, but not sure if this
        // will even cause any performance problems in practice.
        if self.processing_pool.active_count() == 0 {
            for (original_message_meta, handle) in self.handles.drain(..) {
                let handle = handle.into_inner().unwrap();
                let result = handle.join().expect("procspawn failed");
                match result {
                    Ok(data) => {
                        if let Err(MessageRejected {
                            message: transformed_message,
                        }) = self.next_step.submit(original_message_meta.replace(data))
                        {
                            self.message_carried_over = Some(transformed_message);
                        }
                    }
                    Err(e) => {
                        log::error!("Invalid message {:?}", e);
                    }
                }
            }
        }

        log::debug!("processing message,  message={}", message);

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
                let handle = self.processing_pool.spawn(args, |args| {
                    let result = Python::with_gil(|py| -> PyResult<BytesInsertBatch> {
                        let fun: Py<PyAny> = PyModule::import(py, "snuba.consumers.rust_processor")?
                            .getattr("process_rust_message")?
                            .into();

                        let result = fun.call1(py, args)?;
                        let result_decoded: Vec<Vec<u8>> = result.extract(py)?;
                        Ok(BytesInsertBatch {
                            rows: result_decoded,
                        })
                    });

                    result.map_err(|pyerr| {
                        pyerr.to_string()
                    })
                });

                self.handles.push((
                    message.clone().replace(()),
                    Mutex::new(handle)
                ));
            }
        };

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
