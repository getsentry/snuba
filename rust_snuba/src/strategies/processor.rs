use std::collections::BTreeMap;
use std::sync::Arc;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{InvalidMessage, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};
use rust_arroyo::utils::metrics::{get_metrics, BoxMetrics};
use sentry::{Hub, SentryFutureExt};
use sentry_kafka_schemas::{Schema, SchemaError};

use crate::processors::ProcessingFunction;
use crate::types::{BytesInsertBatch, KafkaMessageMetadata};

pub fn make_rust_processor(
    next_step: impl ProcessingStrategy<BytesInsertBatch> + 'static,
    func: ProcessingFunction,
    schema_name: &str,
    enforce_schema: bool,
    concurrency: &ConcurrencyConfig,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let schema = get_schema(schema_name, enforce_schema);
    let metrics = get_metrics();

    let task_runner = MessageProcessor {
        schema,
        enforce_schema,
        metrics,
        func,
    };

    Box::new(RunTaskInThreads::new(
        next_step,
        Box::new(task_runner),
        concurrency,
        Some("process_message"),
    ))
}

fn get_schema(schema_name: &str, enforce_schema: bool) -> Option<Arc<Schema>> {
    match sentry_kafka_schemas::get_schema(schema_name, None) {
        Ok(s) => Some(Arc::new(s)),
        Err(error) => {
            if enforce_schema {
                panic!("Schema error: {error}");
            } else {
                let error: &dyn std::error::Error = &error;
                tracing::error!(error, "Schema error");
            }

            None
        }
    }
}

#[derive(Clone)]
struct MessageProcessor {
    schema: Option<Arc<Schema>>,
    enforce_schema: bool,
    metrics: BoxMetrics,
    func: ProcessingFunction,
}

impl MessageProcessor {
    async fn process_message(
        self,
        message: Message<KafkaPayload>,
    ) -> Result<Message<BytesInsertBatch>, RunTaskError<anyhow::Error>> {
        let msg = match message.inner_message {
            InnerMessage::BrokerMessage(msg) => msg,
            _ => return Err(RunTaskError::Other(anyhow::anyhow!("Unexpected message type"))),
        };

        let maybe_err = RunTaskError::InvalidMessage(InvalidMessage {
            partition: msg.partition,
            offset: msg.offset,
        });

        let kafka_payload = &msg.payload.clone();
        let Some(payload) = kafka_payload.payload() else {
            return Err(maybe_err);
        };

        if let Err(error) = self.validate_schema(payload) {
            let error: &dyn std::error::Error = &error;
            tracing::error!(error, "Failed schema validation");
            return Err(maybe_err);
        };

        self.process_payload(msg).map_err(|error| {
            self.metrics.increment("invalid_message", 1, None);

            sentry::with_scope(
                |scope| {
                    let payload = String::from_utf8_lossy(payload).into();
                    scope.set_extra("payload", payload)
                },
                || {
                    // FIXME: We are double-reporting errors here, as capturing
                    // the error via `tracing::error` will not attach the anyhow
                    // stack trace, but `capture_anyhow` will.
                    sentry::integrations::anyhow::capture_anyhow(&error);
                    let error: &dyn std::error::Error = error.as_ref();
                    tracing::error!(error, "Failed processing message");
                },
            );

            maybe_err
        })
    }

    #[tracing::instrument(skip_all)]
    fn validate_schema(&self, payload: &[u8]) -> Result<(), SchemaError> {
        let Some(schema) = &self.schema else {
            return Ok(());
        };

        let Err(error) = schema.validate_json(payload) else {
            return Ok(());
        };
        self.metrics.increment("schema_validation.failed", 1, None);

        sentry::with_scope(
            |scope| {
                let payload = String::from_utf8_lossy(payload).into();
                scope.set_extra("payload", payload)
            },
            || {
                let error: &dyn std::error::Error = &error;
                tracing::error!(error, "Validation error");
            },
        );

        if !self.enforce_schema {
            Ok(())
        } else {
            Err(error)
        }
    }

    #[tracing::instrument(skip_all)]
    fn process_payload(
        &self,
        msg: BrokerMessage<KafkaPayload>,
    ) -> anyhow::Result<Message<BytesInsertBatch>> {
        let metadata = KafkaMessageMetadata {
            partition: msg.partition.index,
            offset: msg.offset,
            timestamp: msg.timestamp,
        };

        let transformed = (self.func)(msg.payload, metadata)?;

        let payload = BytesInsertBatch::new(
            transformed.rows,
            msg.timestamp,
            transformed.origin_timestamp,
            transformed.sentry_received_timestamp,
            BTreeMap::from([(msg.partition.index, (msg.offset, msg.timestamp))]),
        );
        Ok(Message::new_broker_message(
            payload,
            msg.partition,
            msg.offset,
            msg.timestamp,
        ))
    }
}

impl TaskRunner<KafkaPayload, BytesInsertBatch, anyhow::Error> for MessageProcessor {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<BytesInsertBatch, anyhow::Error> {

        Box::pin(
            self.clone()
                .process_message(message)
                .bind_hub(Hub::new_from_top(Hub::current())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use rust_arroyo::types::{Message, Partition, Topic};

    use crate::types::InsertBatch;
    use crate::Noop;

    #[test]
    fn validate_schema() {
        let partition = Partition::new(Topic::new("test"), 0);
        let concurrency = ConcurrencyConfig::new(5);

        fn noop_processor(
            _payload: KafkaPayload,
            _metadata: KafkaMessageMetadata,
        ) -> anyhow::Result<InsertBatch> {
            Ok(InsertBatch::default())
        }

        let mut strategy =
            make_rust_processor(Noop, noop_processor, "outcomes", true, &concurrency);

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
        let payload = KafkaPayload::new(None, None, Some(payload_str.clone()));
        let message = Message::new_broker_message(payload, partition, 0, Utc::now());

        strategy.submit(message).unwrap(); // Does not error
        strategy.close();
        let _ = strategy.join(None);
    }
}
