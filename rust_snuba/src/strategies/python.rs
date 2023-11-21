use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::Error;

use pyo3::prelude::*;

use crate::types::{BytesInsertBatch, RowData};

use crate::config::MessageProcessorConfig;

enum TaskHandle {
    Procspawn {
        original_message_meta: Message<()>,
        join_handle: Mutex<procspawn::JoinHandle<Result<RowData, String>>>,
    },
    Immediate {
        original_message_meta: Message<()>,
        result: Result<RowData, String>,
    },
}

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    handles: VecDeque<TaskHandle>,
    message_carried_over: Option<Message<BytesInsertBatch>>,
    processing_pool: Option<procspawn::Pool>,
    max_queue_depth: usize,
}

impl PythonTransformStep {
    pub fn new<N>(
        processor_config: MessageProcessorConfig,
        processes: usize,
        max_queue_depth: Option<usize>,
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
            max_queue_depth: max_queue_depth.unwrap_or(processes),
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
        while {
            let active_count = self
                .processing_pool
                .as_ref()
                .map_or(0, |pool| pool.active_count());
            let may_have_finished_handles = active_count <= self.handles.len();
            may_have_finished_handles && !self.handles.is_empty()
        } {
            let (original_message_meta, message_result) = match self.handles.pop_front().unwrap() {
                TaskHandle::Procspawn {
                    original_message_meta,
                    join_handle,
                } => {
                    let handle = join_handle.into_inner().unwrap();
                    let result = handle.join().expect("procspawn failed");
                    (original_message_meta, result)
                }
                TaskHandle::Immediate {
                    original_message_meta,
                    result,
                } => (original_message_meta, result),
            };
            match message_result {
                Ok(data) => {
                    let replacement = BytesInsertBatch {
                        rows: data.rows,
                        // TODO: Actually implement this
                        commit_log_offsets: BTreeMap::from([]),
                    };

                    if let Err(SubmitError::MessageRejected(MessageRejected {
                        message: transformed_message,
                    })) = self
                        .next_step
                        .submit(original_message_meta.replace(replacement))
                    {
                        self.message_carried_over = Some(transformed_message);
                    }
                }
                Err(error) => {
                    tracing::error!(error, "Invalid message");
                }
            }
        }
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.check_for_results();
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        // if there are a lot of "queued" messages (=messages waiting for a free process), let's
        // not enqueue more.
        if let Some(ref processing_pool) = self.processing_pool {
            if processing_pool.queued_count() > self.max_queue_depth {
                tracing::debug!("python strategy provides backpressure");
                return Err(SubmitError::MessageRejected(MessageRejected { message }));
            }
        }

        tracing::debug!(%message, "processing message");

        match message.inner_message {
            InnerMessage::AnyMessage(..) => {
                panic!("AnyMessage cannot be processed");
            }
            InnerMessage::BrokerMessage(BrokerMessage {
                payload,
                offset,
                partition,
                timestamp,
            }) => {
                // TODO: Handle None payload
                let payload_bytes = (payload.payload().unwrap()).clone();

                let args = (payload_bytes, offset, partition.index, timestamp);

                let process_message = |args| {
                    tracing::debug!(?args, "processing message in subprocess");
                    Python::with_gil(|py| -> PyResult<RowData> {
                        let fun: Py<PyAny> =
                            PyModule::import(py, "snuba.consumers.rust_processor")?
                                .getattr("process_rust_message")?
                                .into();

                        let result = fun.call1(py, args)?;
                        let result_decoded: Vec<Vec<u8>> = result.extract(py)?;
                        Ok(RowData {
                            rows: result_decoded,
                        })
                    })
                    .map_err(|pyerr| pyerr.to_string())
                };

                let original_message_meta =
                    Message::new_broker_message((), partition, offset, timestamp);

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

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
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
            None,
            sink.clone(),
        )
        .unwrap();

        let _ = step.poll();
        step.submit(Message::new_broker_message(
            KafkaPayload::new(
                None,
                None,
                Some(br#"{ "timestamp": "2023-03-28T18:50:44.000000Z", "org_id": 1, "project_id": 1, "key_id": 1, "outcome": 1, "reason": "discarded-hash", "event_id": "4ff942d62f3f4d5db9f53b5a015b5fd9", "category": 1, "quantity": 1 }"#.to_vec()),
            ),
            Partition::new(Topic::new("test"), 1),
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
