use std::collections::BTreeMap;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{InvalidMessage, ProcessingStrategy};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};
use rust_arroyo::utils::metrics::get_metrics;

use crate::processors::ProcessingFunction;
use crate::types::{BytesInsertBatch, KafkaMessageMetadata, RowData};

use super::validate_schema::ValidateSchema;

pub fn make_rust_processor(
    next_step: impl ProcessingStrategy<BytesInsertBatch> + 'static,
    func: ProcessingFunction,
    schema_name: &str,
    concurrency: &ConcurrencyConfig,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let task_runner = MessageProcessor { func };
    Box::new(ValidateSchema::new(
        RunTaskInThreads::new(
            next_step,
            Box::new(task_runner),
            concurrency,
            Some("process_message"),
        ),
        schema_name,
        false,
        concurrency,
    ))
}

struct MessageProcessor {
    func: fn(KafkaPayload, KafkaMessageMetadata) -> anyhow::Result<RowData>,
}

impl TaskRunner<KafkaPayload, BytesInsertBatch> for MessageProcessor {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<BytesInsertBatch> {
        let func = self.func;

        Box::pin(async move {
            let broker_message = match message.inner_message {
                InnerMessage::BrokerMessage(msg) => msg,
                _ => panic!("Unexpected message type"),
            };

            let metadata = KafkaMessageMetadata {
                partition: broker_message.partition.index,
                offset: broker_message.offset,
                timestamp: broker_message.timestamp,
            };

            match func(broker_message.payload, metadata) {
                Ok(transformed) => Ok(Message {
                    inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                        payload: BytesInsertBatch::new(
                            broker_message.timestamp,
                            transformed,
                            BTreeMap::from([(
                                broker_message.partition.index,
                                (broker_message.offset, broker_message.timestamp),
                            )]),
                        ),
                        partition: broker_message.partition,
                        offset: broker_message.offset,
                        timestamp: broker_message.timestamp,
                    }),
                }),
                Err(error) => {
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

                    Err(RunTaskError::InvalidMessage(InvalidMessage {
                        partition: broker_message.partition,
                        offset: broker_message.offset,
                    }))
                }
            }
        })
    }
}
