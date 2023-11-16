use std::sync::Arc;
use std::time::Duration;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::{InnerMessage, Message};
use sentry_kafka_schemas;

pub struct SchemaValidator {
    schema: Option<Arc<sentry_kafka_schemas::Schema>>,
    enforce_schema: bool,
}

impl SchemaValidator {
    pub fn new(logical_topic: &str, enforce_schema: bool) -> Self {
        let schema = match sentry_kafka_schemas::get_schema(logical_topic, None) {
            Ok(s) => Some(Arc::new(s)),
            Err(error) => {
                if enforce_schema {
                    panic!("Schema error: {error}");
                } else {
                    tracing::error!(%error, "Schema error");
                }

                None
            }
        };

        SchemaValidator {
            schema,
            enforce_schema,
        }
    }
}

impl TaskRunner<KafkaPayload, KafkaPayload> for SchemaValidator {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<KafkaPayload> {
        let Some(schema) = self.schema.clone() else {
            return Box::pin(async move { Ok(message) });
        };
        let enforce_schema = self.enforce_schema;

        Box::pin(async move {
            // FIXME: this will panic when the payload is empty
            let payload = message.payload().payload.as_ref().unwrap();

            let Err(error) = schema.validate_json(payload) else {
                return Ok(message);
            };

            tracing::error!(%error, "Validation error");
            if !enforce_schema {
                return Ok(message);
            };

            match message.inner_message {
                InnerMessage::BrokerMessage(ref broker_message) => {
                    let partition = broker_message.partition;
                    let offset = broker_message.offset;

                    Err(RunTaskError::InvalidMessage(InvalidMessage {
                        partition,
                        offset,
                    }))
                }
                _ => {
                    panic!("Cannot return Invalid message error");
                }
            }
        })
    }
}

pub struct ValidateSchema {
    inner: RunTaskInThreads<KafkaPayload, KafkaPayload>,
}

impl ValidateSchema {
    pub fn new<N>(
        next_step: N,
        topic: &str,
        enforce_schema: bool,
        concurrency: &ConcurrencyConfig,
    ) -> Self
    where
        N: ProcessingStrategy<KafkaPayload> + 'static,
    {
        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(SchemaValidator::new(topic, enforce_schema)),
            concurrency,
            Some("validate_schema"),
        );

        ValidateSchema { inner }
    }
}

impl ProcessingStrategy<KafkaPayload> for ValidateSchema {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        self.inner.submit(message)
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use rust_arroyo::processing::strategies::{CommitRequest, ProcessingStrategy, SubmitError};
    use rust_arroyo::types::{BrokerMessage, InnerMessage};
    use rust_arroyo::types::{Message, Partition, Topic};
    use std::time::Duration;

    #[test]
    fn validate_schema() {
        let partition = Partition::new(Topic::new("test"), 0);

        struct Noop {}
        impl ProcessingStrategy<KafkaPayload> for Noop {
            fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
            fn submit(
                &mut self,
                _message: Message<KafkaPayload>,
            ) -> Result<(), SubmitError<KafkaPayload>> {
                Ok(())
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

        let concurrency = ConcurrencyConfig::new(5);
        let mut strategy = ValidateSchema::new(Noop {}, "outcomes", true, &concurrency);

        let example = "{
            \"project_id\": 1,
            \"logging.googleapis.com/labels\": {
              \"host\": \"lb-6\"
            },
            \"org_id\": 0,
            \"outcome\": 4,
            \"timestamp\": \"2023-03-28T18:50:39.463685Z\"
          }";

        let payload_str = example.to_string().as_bytes().to_vec();
        let message = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                payload: KafkaPayload {
                    key: None,
                    headers: None,
                    payload: Some(Arc::new(payload_str.clone())),
                },
                partition,
                offset: 0,
                timestamp: Utc::now(),
            }),
        };

        strategy.submit(message).unwrap(); // Does not error
        strategy.close();
        let _ = strategy.join(None);
    }
}
