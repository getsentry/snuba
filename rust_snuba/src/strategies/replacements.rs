use crate::strategies::noop::Noop;
use crate::types::{BytesInsertBatch, InsertOrReplacement};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::Producer;
use rust_arroyo::processing::strategies::merge_commit_request;
use rust_arroyo::processing::strategies::produce::Produce;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, StrategyError, SubmitError,
};
use rust_arroyo::types::{Message, Topic, TopicOrPartition};
use std::time::Duration;

/// Takes messages that are either inserts or replacements.
/// BytesInsertBatch are simply forwarded to the next step.
/// Replacements are produced to the replacement topic.
/// This is  only relevant for the "errors" dataset.
pub struct ProduceReplacements {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    inner: Box<dyn ProcessingStrategy<KafkaPayload>>,
    skip_produce: bool,
}

impl ProduceReplacements {
    pub fn new<N>(
        next_step: N,
        producer: impl Producer<KafkaPayload> + 'static,
        destination: Topic,
        concurrency: &ConcurrencyConfig,
        skip_produce: bool,
    ) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let inner: Box<dyn ProcessingStrategy<KafkaPayload>> = match skip_produce {
            false => Box::new(Produce::new(
                Noop {},
                producer,
                concurrency,
                TopicOrPartition::Topic(destination),
            )),
            _ => Box::new(Noop {}),
        };

        ProduceReplacements {
            next_step: Box::new(next_step),
            inner,
            skip_produce,
        }
    }
}

impl ProcessingStrategy<InsertOrReplacement<BytesInsertBatch>> for ProduceReplacements {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let _ = self.inner.poll(); // Replacement offsets are not committed
        self.next_step.poll()
    }

    fn submit(
        &mut self,
        message: Message<InsertOrReplacement<BytesInsertBatch>>,
    ) -> Result<(), SubmitError<InsertOrReplacement<BytesInsertBatch>>> {
        let payload = message.clone().into_payload();

        match payload {
            InsertOrReplacement::Insert(insert) => {
                // Similar to in python, we don't block submitting inserts
                // even if all pending replacements are not produced yet.
                self.next_step
                    .submit(message.clone().replace(insert))
                    .map_err(|err| match err {
                        SubmitError::MessageRejected(message_rejected) => {
                            let payload = message_rejected.message.into_payload();
                            SubmitError::MessageRejected(MessageRejected {
                                message: message.replace(InsertOrReplacement::Insert(payload)),
                            })
                        }
                        SubmitError::InvalidMessage(invalid) => {
                            SubmitError::InvalidMessage(invalid)
                        }
                    })
            }
            InsertOrReplacement::Replacement(replacement) => {
                if self.skip_produce {
                    tracing::info!("Skipping replacement");
                    return Ok(());
                }

                self.inner
                    .submit(message.clone().replace(replacement.into()))
                    .map_err(|err| match err {
                        SubmitError::MessageRejected(message_rejected) => {
                            let payload = message_rejected.message.into_payload();

                            SubmitError::MessageRejected(MessageRejected {
                                message: message
                                    .replace(InsertOrReplacement::Replacement(payload.into())),
                            })
                        }
                        SubmitError::InvalidMessage(invalid) => {
                            SubmitError::InvalidMessage(invalid)
                        }
                    })
            }
        }
    }

    fn close(&mut self) {
        self.inner.close();
        self.next_step.close();
    }

    fn terminate(&mut self) {
        self.inner.terminate();
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        // We join the produce step with a None timeout as we want to ensure all the replacement
        // messages are produced since we may have already processed / committed those offsets out
        // of order
        let committable = self.inner.join(None)?;
        Ok(merge_commit_request(
            committable,
            self.next_step.join(timeout)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::{MockProducer, TestStrategy};
    use crate::types::RowData;
    use crate::types::{InsertOrReplacement, ReplacementData};
    use chrono::Utc;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn produce_replacements() {
        let next_step = TestStrategy::new();
        let produced_payloads = Arc::new(Mutex::new(vec![]));
        let producer = MockProducer {
            payloads: produced_payloads.clone(),
        };
        let destination = Topic::new("test");
        let concurrency = ConcurrencyConfig::new(10);
        let skip_produce = false;
        let mut strategy =
            ProduceReplacements::new(next_step, producer, destination, &concurrency, skip_produce);

        let row_data: Vec<u8> = vec![];

        strategy
            .submit(Message::new_any_message(
                InsertOrReplacement::Insert(BytesInsertBatch::new(
                    RowData::from_rows(row_data).unwrap(),
                    Utc::now(),
                    None,
                    None,
                    BTreeMap::new(),
                    None,
                )),
                BTreeMap::new(),
            ))
            .unwrap();
        assert_eq!(produced_payloads.lock().unwrap().len(), 0);

        strategy
            .submit(Message::new_any_message(
                InsertOrReplacement::Replacement(ReplacementData {
                    key: "1".as_bytes().to_vec(),
                    value: "{\"project_id\":1}".as_bytes().to_vec(),
                }),
                BTreeMap::new(),
            ))
            .unwrap();

        strategy.poll().unwrap();
        strategy.close();
        strategy.join(None).unwrap();

        assert_eq!(produced_payloads.lock().unwrap().len(), 1);
    }
}
