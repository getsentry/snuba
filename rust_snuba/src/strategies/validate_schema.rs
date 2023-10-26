use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::{InnerMessage, Message};
use sentry_kafka_schemas;
use std::time::Duration;

pub struct SchemaValidator {
    schema: Option<sentry_kafka_schemas::Schema>,
    enforce_schema: bool,
}

impl SchemaValidator {
    pub fn new(logical_topic: String, enforce_schema: bool) -> Self {
        let schema = match sentry_kafka_schemas::get_schema(&logical_topic, None) {
            Ok(s) => Some(s),
            Err(e) => {
                if enforce_schema {
                    panic!("Schema error: {}", e);
                } else {
                    log::error!("Schema error: {}", e);
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
        let mut errored = false;

        if let Some(schema) = &self.schema {
            let payload = message.payload().payload.unwrap();

            let res = schema.validate_json(&payload);

            if let Err(err) = res {
                log::error!("Validation error {}", err);
                if self.enforce_schema {
                    errored = true;
                };
            }
        }

        Box::pin(async move {
            if errored {
                match message.inner_message {
                    InnerMessage::BrokerMessage(ref broker_message) => {
                        let partition = broker_message.partition.clone();
                        let offset = broker_message.offset;

                        Err(InvalidMessage { partition, offset })
                    }
                    _ => {
                        panic!("Cannot return Invalid message error");
                    }
                }
            } else {
                Ok(message)
            }
        })
    }
}

pub struct ValidateSchema {
    inner: Box<dyn ProcessingStrategy<KafkaPayload>>,
}

impl ValidateSchema {
    #[allow(dead_code)]
    pub fn new<N>(next_step: N, topic: String, enforce_schema: bool, concurrency: usize) -> Self
    where
        N: ProcessingStrategy<KafkaPayload> + 'static,
    {
        let inner = Box::new(RunTaskInThreads::new(
            next_step,
            Box::new(SchemaValidator::new(topic, enforce_schema)),
            concurrency,
            Some("validate_schema"),
        ));

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
        let partition = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };

        struct Noop {}
        impl ProcessingStrategy<KafkaPayload> for Noop {
            fn poll(&mut self) -> Option<CommitRequest> {
                None
            }
            fn submit(
                &mut self,
                _message: Message<KafkaPayload>,
            ) -> Result<(), MessageRejected<KafkaPayload>> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
                None
            }
        }

        let mut strategy = ValidateSchema::new(Noop {}, "outcomes".to_string(), true, 5);

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
                    payload: Some(payload_str.clone()),
                },
                partition: partition,
                offset: 0,
                timestamp: Utc::now(),
            }),
        };

        strategy.submit(message).unwrap(); // Does not error
        strategy.close();
        let _ = strategy.join(None);
    }
}
