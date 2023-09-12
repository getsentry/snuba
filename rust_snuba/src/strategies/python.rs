use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};

use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::Error;

use pyo3::prelude::*;

use crate::types::BytesInsertBatch;

use crate::config::MessageProcessorConfig;

enum TaskHandle {
    Procspawn {
        original_message_meta: Message<()>,
        join_handle: Mutex<procspawn::JoinHandle<Result<BytesInsertBatch, String>>>,
    },
    Immediate {
        original_message_meta: Message<()>,
        result: Result<BytesInsertBatch, String>,
    },
}

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    handles: VecDeque<TaskHandle>,
    message_carried_over: Option<Message<BytesInsertBatch>>,
    processing_pool: Option<procspawn::Pool>,
}

impl PythonTransformStep {
    pub fn new<N>(
        processor_config: MessageProcessorConfig,
        processes: usize,
        next_step: N,
    ) -> Result<Self, Error>
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let next_step = Box::new(next_step);
        let python_module = &processor_config.python_module;
        let python_class_name = &processor_config.python_class_name;

        let processing_pool = if processes > 1 {
            Some(
                procspawn::Pool::builder(processes)
                    .env("RUST_SNUBA_PROCESSOR_MODULE", python_module)
                    .env("RUST_SNUBA_PROCESSOR_CLASSNAME", python_class_name)
                    .build()
                    .expect("failed to build procspawn pool"),
            )
        } else {
            Python::with_gil(|py| -> PyResult<()> {
                let fun: Py<PyAny> = PyModule::import(py, "snuba.consumers.rust_processor")?
                    .getattr("initialize_processor")?
                    .into();

                fun.call1(py, (python_module, python_class_name))?;
                Ok(())
            })?;
            None
        };

        Ok(PythonTransformStep {
            next_step,
            handles: VecDeque::new(),
            message_carried_over: None,
            processing_pool,
        })
    }

    fn check_for_results(&mut self) {
        while let Some(handle) = self.handles.pop_front() {
            let (original_message_meta, message_result) = match handle {
                TaskHandle::Procspawn {
                    original_message_meta,
                    join_handle,
                } => {
                    let mut handle = join_handle.into_inner().unwrap();
                    let result = match handle.join_timeout(Duration::ZERO) {
                        Ok(result) => result,
                        Err(e) if e.is_timeout() => {
                            self.handles.push_front(TaskHandle::Procspawn {
                                original_message_meta, join_handle: Mutex::new(handle)
                            });
                            return;
                        }
                        Err(e) => {
                            panic!("procspawn failed: {}", e);
                        }
                    };
                    (original_message_meta, result)
                }
                TaskHandle::Immediate {
                    original_message_meta,
                    result,
                } => (original_message_meta, result),
            };

            match message_result {
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
        if let Some(ref processing_pool) = self.processing_pool {
            if processing_pool.queued_count() > processing_pool.active_count() {
                log::debug!("python strategy provides backpressure");
                return Err(MessageRejected { message });
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

                let process_message = |args| {
                    log::debug!("processing message in subprocess,  args={:?}", args);
                    Python::with_gil(|py| -> PyResult<BytesInsertBatch> {
                        let fun: Py<PyAny> =
                            PyModule::import(py, "snuba.consumers.rust_processor")?
                                .getattr("process_rust_message")?
                                .into();

                        let result = fun.call1(py, args)?;
                        let result_decoded: Vec<Vec<u8>> = result.extract(py)?;
                        Ok(BytesInsertBatch {
                            rows: result_decoded,
                        })
                    }).map_err(|pyerr| pyerr.to_string())
                };

                let original_message_meta = message.clone().replace(());

                if let Some(ref processing_pool) = self.processing_pool {
                    let handle = processing_pool.spawn(args, process_message);

                    self.handles.push_back(TaskHandle::Procspawn {
                        original_message_meta,
                        join_handle: Mutex::new(handle),
                    });
                } else {
                    self.handles.push_back(TaskHandle::Immediate {
                        original_message_meta,
                        result: process_message(args),
                    });
                }
            }
        };

        Ok(())
    }

    fn close(&mut self) {
        self.next_step.close();
    }

    fn terminate(&mut self) {
        if let Some(ref processing_pool) = self.processing_pool {
            processing_pool.kill();
        }
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

    use rust_arroyo::{
        testutils::TestStrategy,
        types::{Partition, Topic},
    };

    fn run_basic(processes: usize) {
        let sink = TestStrategy::new();

        let mut step = PythonTransformStep::new(
            MessageProcessorConfig {
                python_class_name: "OutcomesProcessor".to_owned(),
                python_module: "snuba.datasets.processors.outcomes_processor".to_owned(),
            },
            processes,
            sink.clone(),
        )
        .unwrap();

        let _ = step.poll();
        step.submit(Message::new_broker_message(
            KafkaPayload {
                key: None,
                headers: None,
                payload: Some(br#"{ "timestamp": "2023-03-28T18:50:44.000000Z", "org_id": 1, "project_id": 1, "key_id": 1, "outcome": 1, "reason": "discarded-hash", "event_id": "4ff942d62f3f4d5db9f53b5a015b5fd9", "category": 1, "quantity": 1 }"#.to_vec()),
            },
            Partition {
                topic: Topic {
                    name: "test".to_owned(),
                },
                index: 1,
            },
            1,
            Utc::now(),
        ))
        .unwrap();

        let _ = step.join(Some(Duration::from_secs(10)));

        assert_eq!(sink.messages.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_basic() {
        let handle = procspawn::spawn((), |()| {
            run_basic(1);
        });
        // on macos, this test is polluting something in the global process state causing
        // test_basic_two_processes to hang, therefore isolate it in another subprocess of its own.
        handle.join().unwrap();
    }


    #[test]
    fn test_basic_two_processes() {
        run_basic(2);
    }
}
