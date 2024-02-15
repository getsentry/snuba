use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::counter;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{InvalidMessage, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message, Partition};
use sentry::{Hub, SentryFutureExt};
use sentry_kafka_schemas::{Schema, SchemaError};

use crate::config::ProcessorConfig;
use crate::processors::{ProcessingFunction, ProcessingFunctionWithReplacements};
use crate::types::{BytesInsertBatch, InsertBatch, InsertOrReplacement, KafkaMessageMetadata};

pub fn make_rust_processor(
    next_step: impl ProcessingStrategy<BytesInsertBatch> + 'static,
    func: ProcessingFunction,
    schema_name: &str,
    enforce_schema: bool,
    concurrency: &ConcurrencyConfig,
    processor_config: ProcessorConfig,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let schema = get_schema(schema_name, enforce_schema);

    fn result_to_next_msg(
        transformed: InsertBatch,
        partition: Partition,
        offset: u64,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Message<BytesInsertBatch>> {
        let payload = BytesInsertBatch::new(
            transformed.rows,
            timestamp,
            transformed.origin_timestamp,
            transformed.sentry_received_timestamp,
            BTreeMap::from([(partition.index, (offset, timestamp))]),
            transformed.cogs_data,
        );

        Ok(Message::new_broker_message(
            payload, partition, offset, timestamp,
        ))
    }

    let task_runner = MessageProcessor {
        schema,
        enforce_schema,
        func,
        result_to_next_msg,
        processor_config,
    };

    Box::new(RunTaskInThreads::new(
        next_step,
        Box::new(task_runner),
        concurrency,
        Some("process_message"),
    ))
}

pub fn make_rust_processor_with_replacements(
    next_step: impl ProcessingStrategy<InsertOrReplacement<BytesInsertBatch>> + 'static,
    func: ProcessingFunctionWithReplacements,
    schema_name: &str,
    enforce_schema: bool,
    concurrency: &ConcurrencyConfig,
    processor_config: ProcessorConfig,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let schema = get_schema(schema_name, enforce_schema);

    fn result_to_next_msg(
        transformed: InsertOrReplacement<InsertBatch>,
        partition: Partition,
        offset: u64,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Message<InsertOrReplacement<BytesInsertBatch>>> {
        let payload = match transformed {
            InsertOrReplacement::Insert(transformed) => {
                InsertOrReplacement::Insert(BytesInsertBatch::new(
                    transformed.rows,
                    timestamp,
                    transformed.origin_timestamp,
                    transformed.sentry_received_timestamp,
                    BTreeMap::from([(partition.index, (offset, timestamp))]),
                    transformed.cogs_data,
                ))
            }
            InsertOrReplacement::Replacement(r) => InsertOrReplacement::Replacement(r),
        };

        Ok(Message::new_broker_message(
            payload, partition, offset, timestamp,
        ))
    }

    let task_runner = MessageProcessor {
        schema,
        enforce_schema,
        func,
        result_to_next_msg,
        processor_config,
    };

    Box::new(RunTaskInThreads::new(
        next_step,
        Box::new(task_runner),
        concurrency,
        Some("process_message"),
    ))
}

pub fn get_schema(schema_name: &str, enforce_schema: bool) -> Option<Arc<Schema>> {
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
struct MessageProcessor<TResult: Clone, TNext: Clone> {
    schema: Option<Arc<Schema>>,
    enforce_schema: bool,
    // Convert payload to either InsertBatch (or either insert or replacement for the errors dataset)
    func:
        fn(KafkaPayload, KafkaMessageMetadata, config: &ProcessorConfig) -> anyhow::Result<TResult>,
    // Function that return Message<TNext> to be passed to the next strategy. Gets passed TResult,
    // as well as the message's partition, offset and timestamp.
    result_to_next_msg:
        fn(TResult, Partition, u64, DateTime<Utc>) -> anyhow::Result<Message<TNext>>,
    processor_config: ProcessorConfig,
}

impl<TResult: Clone, TNext: Clone> MessageProcessor<TResult, TNext> {
    async fn process_message(
        self,
        message: Message<KafkaPayload>,
    ) -> Result<Message<TNext>, RunTaskError<anyhow::Error>> {
        if let Err(error) = validate_schema(&message, &self.schema, self.enforce_schema) {
            counter!("schema_validation.failed");
            return Err(error);
        };

        let msg = match message.inner_message {
            InnerMessage::BrokerMessage(msg) => msg,
            _ => {
                return Err(RunTaskError::Other(anyhow::anyhow!(
                    "Unexpected message type"
                )))
            }
        };

        let maybe_err = RunTaskError::InvalidMessage(InvalidMessage {
            partition: msg.partition,
            offset: msg.offset,
        });

        let kafka_payload = &msg.payload.clone();
        let Some(payload) = kafka_payload.payload() else {
            return Err(maybe_err);
        };

        self.process_payload(msg).map_err(|error| {
            counter!("invalid_message");

            sentry::with_scope(
                |scope| {
                    let payload = String::from_utf8_lossy(payload);
                    let payload_as_value: serde_json::Value =
                        serde_json::from_str(&payload).unwrap_or(payload.into());
                    scope.set_extra("payload", payload_as_value);
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
    fn process_payload(&self, msg: BrokerMessage<KafkaPayload>) -> anyhow::Result<Message<TNext>> {
        let metadata = KafkaMessageMetadata {
            partition: msg.partition.index,
            offset: msg.offset,
            timestamp: msg.timestamp,
        };

        let transformed = (self.func)(msg.payload, metadata, &self.processor_config)?;

        (self.result_to_next_msg)(transformed, msg.partition, msg.offset, msg.timestamp)
    }
}

impl<TResult: Clone + 'static, TNext: Clone + 'static>
    TaskRunner<KafkaPayload, TNext, anyhow::Error> for MessageProcessor<TResult, TNext>
{
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<TNext, anyhow::Error> {
        Box::pin(
            self.clone()
                .process_message(message)
                .bind_hub(Hub::new_from_top(Hub::current())),
        )
    }
}

pub fn validate_schema(
    message: &Message<KafkaPayload>,
    schema: &Option<Arc<Schema>>,
    enforce_schema: bool,
) -> Result<(), RunTaskError<anyhow::Error>> {
    let msg = match &message.inner_message {
        InnerMessage::BrokerMessage(msg) => msg,
        _ => {
            return Err(RunTaskError::Other(anyhow::anyhow!(
                "Unexpected message type"
            )))
        }
    };

    let maybe_err = RunTaskError::InvalidMessage(InvalidMessage {
        partition: msg.partition,
        offset: msg.offset,
    });

    let kafka_payload = &msg.payload.clone();
    let Some(payload) = kafka_payload.payload() else {
        return Err(maybe_err);
    };

    if let Err(error) = _validate_schema(schema, enforce_schema, payload) {
        let error: &dyn std::error::Error = &error;
        tracing::error!(error, "Failed schema validation");
        return Err(maybe_err);
    };

    Ok(())
}

#[tracing::instrument(skip_all)]
fn _validate_schema(
    schema: &Option<Arc<Schema>>,
    enforce_schema: bool,
    payload: &[u8],
) -> Result<(), SchemaError> {
    let Some(schema) = &schema else {
        return Ok(());
    };

    let Err(error) = schema.validate_json(payload) else {
        return Ok(());
    };
    counter!("schema_validation.failed");

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

    if !enforce_schema {
        Ok(())
    } else {
        Err(error)
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
            _config: &ProcessorConfig,
        ) -> anyhow::Result<InsertBatch> {
            Ok(InsertBatch::default())
        }

        let mut strategy = make_rust_processor(
            Noop,
            noop_processor,
            "outcomes",
            true,
            &concurrency,
            ProcessorConfig::default(),
        );

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
