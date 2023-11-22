use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message, Partition};

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Error;
use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use crate::config::MessageProcessorConfig;
use crate::types::{BytesInsertBatch, RowData};

struct MessageMeta {
    partition: Partition,
    offset: u64,
    timestamp: DateTime<Utc>,
}

enum TaskHandle {
    Procspawn {
        original_message_meta: MessageMeta,
        join_handle: Mutex<procspawn::JoinHandle<Result<RowData, String>>>,
        submit_timestamp: SystemTime,
    },
    Immediate {
        original_message_meta: MessageMeta,
        result: Result<RowData, String>,
        submit_timestamp: SystemTime,
    },
}

impl TaskHandle {
    fn submit_timestamp(&self) -> SystemTime {
        match self {
            TaskHandle::Procspawn {
                submit_timestamp, ..
            } => *submit_timestamp,
            TaskHandle::Immediate {
                submit_timestamp, ..
            } => *submit_timestamp,
        }
    }
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
        if let Some(message_carried_over) = self.message_carried_over.take() {
            if let Err(SubmitError::MessageRejected(MessageRejected { message })) =
                self.next_step.submit(message_carried_over)
            {
                self.message_carried_over = Some(message);
                return;
            }
        }

        debug_assert!(self.message_carried_over.is_none());

        let (original_message_meta, message_result) = match self.handles.pop_front() {
            Some(TaskHandle::Procspawn {
                original_message_meta,
                join_handle,
                ..
            }) => {
                let handle = join_handle.into_inner().unwrap();
                let result = handle.join().expect("procspawn failed");
                (original_message_meta, result)
            }
            Some(TaskHandle::Immediate {
                original_message_meta,
                result,
                ..
            }) => (original_message_meta, result),
            None => return,
        };

        match message_result {
            Ok(rows) => {
                let replacement = BytesInsertBatch::new(
                    original_message_meta.timestamp,
                    rows,
                    // TODO: Actually implement this
                    BTreeMap::new(),
                );

                let new_message = Message::new_broker_message(
                    replacement,
                    original_message_meta.partition,
                    original_message_meta.offset,
                    original_message_meta.timestamp,
                );

                if let Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) = self.next_step.submit(new_message)
                {
                    self.message_carried_over = Some(transformed_message);
                }
            }
            Err(error) => {
                tracing::error!(error, "Invalid message");
            }
        }
    }

    fn queue_needs_drain(&self) -> bool {
        if self.handles.len() > self.max_queue_depth {
            return true;
        }

        if let Some(handle) = self.handles.front() {
            if let Ok(elapsed) = handle.submit_timestamp().elapsed() {
                if elapsed > Duration::from_secs(10) {
                    return true;
                }
            }
        }

        false
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        while self.queue_needs_drain() {
            self.check_for_results();
        }

        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        // if there are a lot of "queued" messages (=messages waiting for a free process), let's
        // not enqueue more.
        if self.queue_needs_drain() {
            tracing::debug!("python strategy provides backpressure");
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        tracing::debug!(%message, "processing message");

        match message.inner_message {
            InnerMessage::AnyMessage(..) => {
                // Snuba message processors, as their interface is defined in Python, expect a
                // single, specific partition/offset combination for every payload.
                panic!("AnyMessage cannot be processed by a message processor");
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

                let process_message = |args: (_, _, _, DateTime<Utc>)| {
                    tracing::debug!(?args, "processing message in subprocess");
                    Python::with_gil(|py| -> PyResult<RowData> {
                        let fun: Py<PyAny> =
                            PyModule::import(py, "snuba.consumers.rust_processor")?
                                .getattr("process_rust_message")?
                                .into();

                        let result = fun.call1(py, args)?;
                        let result_decoded: Vec<Vec<u8>> = result.extract(py)?;
                        Ok(RowData::from_rows(result_decoded))
                    })
                    .map_err(|pyerr| pyerr.to_string())
                };

                let original_message_meta = MessageMeta {
                    partition,
                    offset,
                    timestamp,
                };

                let submit_timestamp = SystemTime::now();

                if let Some(ref processing_pool) = self.processing_pool {
                    let handle = processing_pool.spawn(args, process_message);

                    self.handles.push_back(TaskHandle::Procspawn {
                        submit_timestamp,
                        original_message_meta,
                        join_handle: Mutex::new(handle),
                    });
                } else {
                    self.handles.push_back(TaskHandle::Immediate {
                        submit_timestamp,
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

    struct BackpressureForever;

    impl<T> ProcessingStrategy<T> for BackpressureForever {
        fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
            Ok(None)
        }

        fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
            Err(SubmitError::MessageRejected(MessageRejected { message }))
        }

        fn close(&mut self) {}
        fn terminate(&mut self) {}
        fn join(
            &mut self,
            _timeout: Option<Duration>,
        ) -> Result<Option<CommitRequest>, InvalidMessage> {
            Ok(None)
        }
    }

    #[test]
    fn test_backpressure() {
        let mut step = PythonTransformStep::new(
            MessageProcessorConfig {
                python_class_name: "OutcomesProcessor".to_owned(),
                python_module: "snuba.datasets.processors.outcomes_processor".to_owned(),
            },
            2,
            None,
            BackpressureForever,
        )
        .unwrap();

        step.poll().unwrap();
        step.submit(Message::new_broker_message(
            KafkaPayload::new(
                None,
                None,
                Some(br#"{ "timestamp": "2023-03-28T18:50:44.000000Z", "org_id": 1, "project_id": 1, "key_id": 1, "outcome": 1, "reason": "discarded-hash", "event_id": "4ff942d62f3f4d5db9f53b5a015b5fd9", "category": 1, "quantity": 1 }"#.to_vec()),
            ),
            Partition::new(Topic::new("test"), 1),
            1,
            Utc::now(),
        )).unwrap();

        step.join(None).unwrap();

        assert!(matches!(step.submit(Message::new_broker_message(
            KafkaPayload::new(
                None,
                None,
                Some(br#"{ "timestamp": "2023-03-28T18:50:44.000000Z", "org_id": 1, "project_id": 1, "key_id": 1, "outcome": 1, "reason": "discarded-hash", "event_id": "4ff942d62f3f4d5db9f53b5a015b5fd9", "category": 1, "quantity": 1 }"#.to_vec()),
            ),
            Partition::new(Topic::new("test"), 1),
            1,
            Utc::now(),
        )).unwrap_err(), SubmitError::MessageRejected(_)));
    }
}
