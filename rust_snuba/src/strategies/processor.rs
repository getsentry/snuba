use std::collections::BTreeMap;
use std::sync::Arc;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{InvalidMessage, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};
use rust_arroyo::utils::metrics::{get_metrics, BoxMetrics};
use sentry_kafka_schemas::{Schema, SchemaError};

use crate::processors::ProcessingFunction;
use crate::types::{BytesInsertBatch, KafkaMessageMetadata, RowData};

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
                tracing::error!(%error, "Schema error");
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
    func: fn(KafkaPayload, KafkaMessageMetadata) -> anyhow::Result<RowData>,
}

impl MessageProcessor {
    async fn process_message(
        self,
        msg: BrokerMessage<KafkaPayload>,
    ) -> Result<Message<BytesInsertBatch>, RunTaskError> {
        // FIXME: this will panic when the payload is empty
        let payload = msg.payload.payload().unwrap();

        let maybe_err = RunTaskError::InvalidMessage(InvalidMessage {
            partition: msg.partition,
            offset: msg.offset,
        });

        if self.validate_schema(payload).is_err() {
            return Err(maybe_err);
        };

        self.process_payload(msg).map_err(|error| {
            // TODO: after moving to `tracing`, we can properly attach `err` to the log.
            // however, as Sentry captures `error` logs as errors by default,
            // we would double-log this error here:
            tracing::error!(%error, "Failed processing message");
            let metrics = get_metrics();
            metrics.increment("invalid_message", 1, None);
            sentry::with_scope(
                |_scope| {
                    // FIXME(swatinem): we already moved `broker_message.payload`
                    // let payload = broker_message
                    //     .payload
                    //     .payload
                    //     .as_deref()
                    //     .map(String::from_utf8_lossy)
                    //     .into();
                    // scope.set_extra("payload", payload)
                },
                || {
                    sentry::integrations::anyhow::capture_anyhow(&error);
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

        tracing::error!(%error, "Validation error");
        self.metrics.increment("schema_validation.failed", 1, None);

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
            msg.timestamp,
            transformed,
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

impl TaskRunner<KafkaPayload, BytesInsertBatch> for MessageProcessor {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<BytesInsertBatch> {
        let broker_message = match message.inner_message {
            InnerMessage::BrokerMessage(msg) => msg,
            _ => panic!("Unexpected message type"),
        };

        Box::pin(self.clone().process_message(broker_message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use rust_arroyo::types::{Message, Partition, Topic};

    use crate::Noop;

    #[test]
    fn validate_schema() {
        let partition = Partition::new(Topic::new("test"), 0);
        let concurrency = ConcurrencyConfig::new(5);

        fn noop_processor(
            _payload: KafkaPayload,
            _metadata: KafkaMessageMetadata,
        ) -> anyhow::Result<RowData> {
            Ok(RowData::from_rows([]))
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
