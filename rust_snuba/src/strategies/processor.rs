use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Utc};
use regex::Regex;
use sentry::{Hub, SentryFutureExt};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use sentry_arroyo::processing::strategies::{InvalidMessage, ProcessingStrategy};
use sentry_arroyo::types::{BrokerMessage, InnerMessage, Message, Partition};
use sentry_kafka_schemas::{Schema, SchemaError, SchemaType};

use crate::config::ProcessorConfig;
use crate::processors::{ProcessingFunction, ProcessingFunctionWithReplacements};
use crate::types::{
    BytesInsertBatch, CommitLogEntry, CommitLogOffsets, InsertBatch, InsertOrReplacement,
    KafkaMessageMetadata, RowData,
};

pub fn make_rust_processor(
    next_step: impl ProcessingStrategy<BytesInsertBatch<RowData>> + 'static,
    func: ProcessingFunction,
    schema_name: &str,
    enforce_schema: bool,
    concurrency: &ConcurrencyConfig,
    processor_config: ProcessorConfig,
    stop_at_timestamp: Option<i64>,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let schema = get_schema(schema_name, enforce_schema);

    fn result_to_next_msg(
        transformed: InsertBatch,
        partition: Partition,
        offset: u64,
        timestamp: DateTime<Utc>,
        stop_at_timestamp: Option<i64>,
    ) -> anyhow::Result<Message<BytesInsertBatch<RowData>>> {
        // Don't process any more messages
        if let Some(stop) = stop_at_timestamp {
            if stop < timestamp.timestamp() {
                let payload = BytesInsertBatch::default();
                return Ok(Message::new_broker_message(
                    payload, partition, offset, timestamp,
                ));
            }
        }

        let payload = BytesInsertBatch::new(
            transformed.rows,
            Some(timestamp),
            transformed.origin_timestamp,
            transformed.sentry_received_timestamp,
            CommitLogOffsets(BTreeMap::from([(
                partition.index,
                CommitLogEntry {
                    offset,
                    orig_message_ts: timestamp,
                    received_p99: transformed.origin_timestamp.into_iter().collect(),
                },
            )])),
            transformed.cogs_data.unwrap_or_default(),
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
        stop_at_timestamp,
    };

    Box::new(RunTaskInThreads::new(
        next_step,
        task_runner,
        concurrency,
        Some("process_message"),
    ))
}

pub fn make_rust_processor_with_replacements(
    next_step: impl ProcessingStrategy<InsertOrReplacement<BytesInsertBatch<RowData>>> + 'static,
    func: ProcessingFunctionWithReplacements,
    schema_name: &str,
    enforce_schema: bool,
    concurrency: &ConcurrencyConfig,
    processor_config: ProcessorConfig,
    stop_at_timestamp: Option<i64>,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let schema = get_schema(schema_name, enforce_schema);

    fn result_to_next_msg(
        transformed: InsertOrReplacement<InsertBatch>,
        partition: Partition,
        offset: u64,
        timestamp: DateTime<Utc>,
        stop_at_timestamp: Option<i64>,
    ) -> anyhow::Result<Message<InsertOrReplacement<BytesInsertBatch<RowData>>>> {
        // Don't process any more messages
        if let Some(stop) = stop_at_timestamp {
            if stop < timestamp.timestamp() {
                let payload = BytesInsertBatch::default();
                return Ok(Message::new_broker_message(
                    InsertOrReplacement::Insert(payload),
                    partition,
                    offset,
                    timestamp,
                ));
            }
        }

        let payload = match transformed {
            InsertOrReplacement::Insert(transformed) => {
                InsertOrReplacement::Insert(BytesInsertBatch::new(
                    transformed.rows,
                    Some(timestamp),
                    transformed.origin_timestamp,
                    transformed.sentry_received_timestamp,
                    CommitLogOffsets(BTreeMap::from([(
                        partition.index,
                        CommitLogEntry {
                            offset,
                            orig_message_ts: timestamp,
                            received_p99: transformed.origin_timestamp.into_iter().collect(),
                        },
                    )])),
                    transformed.cogs_data.unwrap_or_default(),
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
        stop_at_timestamp,
    };

    Box::new(RunTaskInThreads::new(
        next_step,
        task_runner,
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
    #[allow(clippy::type_complexity)]
    result_to_next_msg:
        fn(TResult, Partition, u64, DateTime<Utc>, Option<i64>) -> anyhow::Result<Message<TNext>>,
    processor_config: ProcessorConfig,
    stop_at_timestamp: Option<i64>,
}

impl<TResult: Clone, TNext: Clone> MessageProcessor<TResult, TNext> {
    async fn process_message(
        self,
        message: Message<KafkaPayload>,
    ) -> Result<Message<TNext>, RunTaskError<anyhow::Error>> {
        let start_time = Instant::now();
        validate_schema(&message, &self.schema, self.enforce_schema)?;

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

        record_message_stats(payload);

        let processed_message = self.process_payload(msg).map_err(|error| {
            counter!("invalid_message");

            sentry::with_scope(
                |scope| {
                    let payload = String::from_utf8_lossy(payload);
                    let payload_as_value: serde_json::Value =
                        serde_json::from_str(&payload).unwrap_or(payload.into());
                    scope.set_extra("payload", payload_as_value);
                },
                || {
                    let error: &dyn std::error::Error = error.as_ref();
                    tracing::error!(error, "Failed processing message");
                },
            );

            maybe_err
        });

        let elapsed = start_time.elapsed();
        counter!("message_processing_time_ms", elapsed.as_millis() as i64);

        processed_message
    }

    #[tracing::instrument(skip_all)]
    fn process_payload(&self, msg: BrokerMessage<KafkaPayload>) -> anyhow::Result<Message<TNext>> {
        let metadata = KafkaMessageMetadata {
            partition: msg.partition.index,
            offset: msg.offset,
            timestamp: msg.timestamp,
        };

        let transformed = (self.func)(msg.payload, metadata, &self.processor_config)?;

        (self.result_to_next_msg)(
            transformed,
            msg.partition,
            msg.offset,
            msg.timestamp,
            self.stop_at_timestamp,
        )
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

    let Err(error) = (match schema.schema_type {
        SchemaType::Protobuf => schema.validate_protobuf(payload).map(drop),
        SchemaType::Json => schema.validate_json(payload).map(drop),
        _ => Err(SchemaError::InvalidType),
    }) else {
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

static IP_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    let ipv4 = r"(?x)(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.
                  (?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.
                  (?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.
                  (?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])";

    Regex::new(ipv4).unwrap()
});

fn record_message_stats(payload: &[u8]) {
    // for context see INC-984 -- the idea is to have a canary metric for whenever we inadvertently
    // start ingesting much more PII into our systems. this does not prevent PII leakage but might
    // improve our response time to the leak.
    if let Ok(string) = std::str::from_utf8(payload) {
        if IP_REGEX.is_match(string) {
            counter!("message_stats.has_ipv4_pattern", 1, "outcome" => "true");
        } else {
            counter!("message_stats.has_ipv4_pattern", 1, "outcome" => "false");
        }
    } else {
        counter!("message_stats.has_ipv4_pattern", 1, "outcome" => "invalid_payload");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use sentry_arroyo::backends::kafka::types::KafkaPayload;
    use sentry_arroyo::types::{Message, Partition, Topic};

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
            None,
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
        let _ = strategy.join(None);
    }

    #[test]
    fn test_ip_addresses() {
        let test_cases = [
            ("192.168.0.1", true),
            ("255.255.255.255", true),
            ("0.0.0.0", true),
            ("2001:0db8:85a3:0000:0000:8a2e:0370:7334", false), // Valid IPv6, but we don't handle IPv6 yet
            ("::1", false),
            ("fe80::1%lo0", false),
            ("invalid192.168.0.1", true),
            ("invalid", false),
        ];

        for (address, is_ipv4) in test_cases {
            assert_eq!(
                IP_REGEX.is_match(address),
                is_ipv4,
                "{} failed IPv4 validation",
                address
            );
        }
    }
}
