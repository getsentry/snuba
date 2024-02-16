use crate::config::MessageProcessorConfig;

use crate::types::{BytesInsertBatch, CogsData, CommitLogEntry, CommitLogOffsets, RowData};
use anyhow::Error;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use pyo3::prelude::*;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    StrategyError, SubmitError,
};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
use rust_arroyo::utils::timing::Deadline;
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::sync::Arc;
use std::time::Duration;

type ReturnValue = (Vec<Vec<u8>>, Option<DateTime<Utc>>, Option<DateTime<Utc>>);
type MessageTimestamp = DateTime<Utc>;
type Committable = BTreeMap<(String, u16), u64>;
type PyReturnValue = (ReturnValue, MessageTimestamp, Committable);

pub struct PythonTransformStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    python_strategy: Arc<Mutex<Py<PyAny>>>,
    transformed_messages: VecDeque<Message<BytesInsertBatch>>,
    commit_request_carried_over: Option<CommitRequest>,
}

impl PythonTransformStep {
    pub fn new<N>(
        next_step: N,
        processor_config: MessageProcessorConfig,
        concurrency: usize,
        max_queue_depth: Option<usize>,
    ) -> Result<Self, Error>
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        env::set_var(
            "RUST_SNUBA_PROCESSOR_MODULE",
            processor_config.python_module.clone(),
        );
        env::set_var(
            "RUST_SNUBA_PROCESSOR_CLASSNAME",
            processor_config.python_class_name.clone(),
        );

        let instance = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let module = PyModule::import(py, "snuba.consumers.rust_processor")?;
            let cls: Py<PyAny> = module.getattr("RunPythonMultiprocessing")?.into();
            cls.call1(py, (concurrency, max_queue_depth.unwrap_or(concurrency)))
        })?;

        Ok(Self {
            next_step: Box::new(next_step),
            python_strategy: Arc::new(Mutex::new(instance)),
            transformed_messages: VecDeque::new(),
            commit_request_carried_over: None,
        })
    }
    fn handle_py_return_value(&mut self, messages: Vec<PyReturnValue>) {
        // Used internally by "poll" and "join" to populate `self.transformed_messages` from Python return values
        for py_message in messages {
            let (
                (payload, origin_timestamp, sentry_received_timestamp),
                message_timestamp,
                offsets,
            ) = py_message;

            let commit_log_offsets = offsets
                .iter()
                .map(|((_t, p), o)| {
                    (
                        *p,
                        CommitLogEntry {
                            offset: *o,
                            orig_message_ts: message_timestamp,
                            received_p99: Vec::new(),
                        },
                    )
                })
                .collect();

            let payload = BytesInsertBatch::new(
                RowData::from_encoded_rows(payload),
                message_timestamp,
                origin_timestamp,
                sentry_received_timestamp,
                CommitLogOffsets(commit_log_offsets),
                CogsData::default(),
            );

            let mut committable: BTreeMap<Partition, u64> = BTreeMap::new();
            for ((t, p), o) in offsets {
                committable.insert(Partition::new(Topic::new(&t), p), o);
            }

            let message = Message::new_any_message(payload, committable);
            self.transformed_messages.push_back(message);
        }
    }
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let messages = Python::with_gil(|py| -> PyResult<Vec<PyReturnValue>> {
            let python_strategy = self.python_strategy.lock();
            let result = python_strategy.call_method0(py, "poll")?;
            Ok(result.extract(py).unwrap())
        })
        .unwrap();

        self.handle_py_return_value(messages);

        // Attempt to submit all transformed messages
        while let Some(msg) = self.transformed_messages.pop_front() {
            let commit_request = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);

            match self.next_step.submit(msg) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.transformed_messages.push_front(transformed_message);
                    break;
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message.into());
                }
                Ok(_) => {}
            }
        }

        let commit_request = self.next_step.poll()?;

        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            commit_request,
        ))
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        if self.transformed_messages.len() > 100_000 {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let mut apply_backpressure = false;
        let mut invalid_message: Option<InvalidMessage> = None;

        Python::with_gil(|py| -> PyResult<()> {
            let python_strategy = self.python_strategy.lock();
            match message.clone().inner_message {
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
                    // XXX: Python message processors do not support null payload, even though this is valid in
                    // Kafka so we convert it to an empty vec.
                    let payload_bytes = payload.payload().cloned().unwrap_or_default();
                    let args = (
                        payload_bytes,
                        partition.topic.as_str(),
                        offset,
                        partition.index,
                        timestamp,
                    );

                    let rv: (u8, Option<InvalidMessageMetadata>) = python_strategy
                        .call_method1(py, "submit", args)?
                        .extract(py)?;

                    match rv {
                        (1, _) => {
                            apply_backpressure = true;
                        }
                        (2, meta) => {
                            let metadata = meta.unwrap();
                            invalid_message = Some(InvalidMessage {
                                partition: Partition {
                                    topic: Topic::new(&metadata.topic),
                                    index: metadata.partition,
                                },
                                offset: metadata.offset,
                            });
                        }
                        _ => {
                            tracing::debug!("successfully submitted to multiprocessing");
                        }
                    }
                }
            };
            Ok(())
        })
        .unwrap();

        if let Some(msg) = invalid_message {
            return Err(SubmitError::InvalidMessage(msg));
        }

        if apply_backpressure {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(Deadline::new);
        let timeout_secs = timeout.map(|d| d.as_secs());

        let messages = Python::with_gil(|py| -> PyResult<Vec<PyReturnValue>> {
            let python_strategy = self.python_strategy.lock();
            let result = python_strategy
                .call_method1(py, "join", (timeout_secs,))
                .unwrap();

            Ok(result.extract(py).unwrap())
        })
        .unwrap();

        self.handle_py_return_value(messages);

        // Attempt to submit all transformed messages up to the deadline
        while let Some(msg) = self.transformed_messages.pop_front() {
            let commit_request = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);

            match self.next_step.submit(msg) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.transformed_messages.push_front(transformed_message);
                    if deadline.map_or(false, |d| d.has_elapsed()) {
                        tracing::warn!("Timeout reached");
                        break;
                    }
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message.into());
                }
                Ok(_) => {}
            }
        }

        self.next_step.close();
        let next_commit = self.next_step.join(timeout)?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[derive(Debug)]
struct InvalidMessageMetadata {
    pub topic: String,
    pub partition: u16,
    pub offset: u64,
}

impl FromPyObject<'_> for InvalidMessageMetadata {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        Ok(InvalidMessageMetadata {
            topic: dict.get_item("topic")?.extract()?,
            partition: dict.get_item("partition")?.extract()?,
            offset: dict.get_item("offset")?.extract()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_arroyo::testutils::TestStrategy;

    #[test]
    fn test_python() {
        crate::testutils::initialize_python();

        let sink = TestStrategy::new();

        let processor_config = MessageProcessorConfig {
            python_class_name: "OutcomesProcessor".to_owned(),
            python_module: "snuba.datasets.processors.outcomes_processor".to_owned(),
        };

        let mut step = PythonTransformStep::new(sink.clone(), processor_config, 1, None).unwrap();
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
        step.poll().unwrap();

        let _ = step.join(Some(Duration::from_secs(10)));

        assert_eq!(sink.messages.lock().unwrap().len(), 1);
    }
}
