use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};

use std::collections::VecDeque;
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::sync::Mutex;

use anyhow::Error;

use pyo3::prelude::*;

use crate::types::BytesInsertBatch;

use crate::config::MessageProcessorConfig;

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    handles: VecDeque<(
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
            handles: VecDeque::new(),
            message_carried_over: None,
            processing_pool: procspawn::Pool::builder(processes)
                .env("RUST_SNUBA_PROCESSOR_MODULE", python_module)
                .env("RUST_SNUBA_PROCESSOR_CLASSNAME", python_class_name)
                .build().expect("failed to build procspawn pool"),
        })
    }

    fn check_for_results(&mut self) {
        // procspawn has no join() timeout that does not consume the handle on timeout.
        //
        // Additionally we have observed, at least on MacOS, that procspawn's active_count() only
        // decreases when the handle is consumed. Therefore our count of actually saturated
        // processes is `self.processing_pool.active_count() - self.handles.len()`.
        //
        // If no process is saturated (i.e. above equation is <= 0), we can conclude that all tasks
        // are done and all handles can be joined and consumed without waiting.
        while self.processing_pool.active_count() <= self.handles.len() && !self.handles.is_empty() {
            let (original_message_meta, handle) = self.handles.pop_front().unwrap();
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
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.check_for_results();
        self.next_step.poll()
    }

    fn submit(
        &mut self,
        message: Message<KafkaPayload>,
    ) -> Result<(), MessageRejected<KafkaPayload>> {
        self.check_for_results();

        // if there are a lot of "queued" messages (=messages waiting for a free process), let's
        // not enqueue more.
        //
        // this threshold was chosen arbitrarily with no performance measuring, and we may also
        // check for queued_count() > 0 instead, but the rough idea of comparing with
        // active_count() is that we allow for one pending message per process, so that procspawn
        // is able to keep CPU saturation high.
        if self.processing_pool.queued_count() > self.processing_pool.active_count() {
            log::debug!("python strategy provides backpressure");
            return Err(MessageRejected { message });
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
                    log::debug!("processing message in subprocess,  args={:?}", args);
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

                    Ok(result.unwrap())
                });

                self.handles.push_back((
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
        self.processing_pool.kill();
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        let now = Instant::now();

        let deadline = timeout.map(|x| now + x);

        // while deadline has not yet passed
        while deadline.map_or(true, |x| x.elapsed().is_zero()) && !self.handles.is_empty() {
            self.check_for_results();
            sleep(Duration::from_millis(10));
        }

        // TODO: we need to shut down the python module properly in order to avoid dataloss in
        // sentry sdk or similar things that run in python's atexit
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
procspawn::enable_test_support!();


#[cfg(test)]
mod tests {

    use super::*;

    use chrono::Utc;

    use rust_arroyo::{testutils::TestStrategy, types::{Topic, Partition}};

    #[test]
    fn test_basic() {
        let sink = TestStrategy::new();

        let mut step = PythonTransformStep::new(MessageProcessorConfig {
            python_class_name: "IdentityProcessor".to_owned(),
            python_module: "tests.rust_helpers".to_owned()
        }, 1, sink.clone()).unwrap();

        step.poll();
        step.submit(Message::new_broker_message(
            KafkaPayload {
                key: None,
                headers: None,
                payload: Some(br#"{"hello": "world"}"#.to_vec()),
            },
            Partition {
                topic: Topic {
                    name: "test".to_owned(),
                },
                index: 1,
            },
            1,
            Utc::now(),
        )).unwrap();

        step.join(Some(Duration::from_secs(10)));

        assert_eq!(sink.messages.lock().unwrap().len(), 1);
    }
}
