use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::InvalidMessage;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::Message;
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
        let mut errorred = false;

        if let Some(schema) = &self.schema {
            let payload = message.payload().payload.unwrap();

            let res = schema.validate_json(&payload);

            if let Err(err) = res {
                log::error!("Validation error {}", err);
                if self.enforce_schema {
                    errorred = true;
                };
            }
        }

        Box::pin(async move {
            if errorred {
                Err(InvalidMessage)
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
    pub fn new<N>(next_step: N, topic: String, enforce_schema: bool) -> Self
    where
        N: ProcessingStrategy<KafkaPayload> + 'static,
    {
        let concurrency = 5;
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
    fn poll(&mut self) -> Option<CommitRequest> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<KafkaPayload>,
    ) -> Result<(), MessageRejected<KafkaPayload>> {
        self.inner.submit(message)
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::Produce;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
    use crate::types::{BrokerMessage, InnerMessage};
    use crate::types::{Message, Partition, Topic, TopicOrPartition};
    use chrono::Utc;
    use std::time::Duration;

    #[test]
    fn validate_schema() {
        let config = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my_group".to_string(),
            "latest".to_string(),
            false,
            None,
        );

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

        let mut strategy = ValidateSchema::new(
            Noop {},
            10,
            TopicOrPartition::Topic(partition.topic.clone()),
        );

        let payload_str = "hello world".to_string().as_bytes().to_vec();
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

        strategy.submit(message).unwrap();
        strategy.close();
        let _ = strategy.join(None);
    }
}
