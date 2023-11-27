use crate::config;
use crate::processors;
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::python::PythonTransformStep;
use crate::strategies::validate_schema::ValidateSchema;
use crate::types::{BytesInsertBatch, KafkaMessageMetadata, RowData};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::InvalidMessage;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};
use rust_arroyo::utils::metrics::get_metrics;
use std::collections::BTreeMap;
use std::sync::Arc;

use std::time::Duration;

pub struct ConsumerStrategyFactory {
    storage_config: config::StorageConfig,
    logical_topic_name: String,
    max_batch_size: usize,
    max_batch_time: Duration,
    skip_write: bool,
    concurrency: ConcurrencyConfig,
    python_max_queue_depth: Option<usize>,
    use_rust_processor: bool,
}

impl ConsumerStrategyFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_config: config::StorageConfig,
        logical_topic_name: String,
        max_batch_size: usize,
        max_batch_time: Duration,
        skip_write: bool,
        concurrency: ConcurrencyConfig,
        python_max_queue_depth: Option<usize>,
        use_rust_processor: bool,
    ) -> Self {
        Self {
            storage_config,
            logical_topic_name,
            max_batch_size,
            max_batch_time,
            skip_write,
            concurrency,
            python_max_queue_depth,
            use_rust_processor,
        }
    }
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

impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let accumulator = Arc::new(BytesInsertBatch::merge);

        let clickhouse_concurrency = ConcurrencyConfig::with_runtime(
            self.concurrency.concurrency,
            self.concurrency.handle(),
        );
        let next_step = Reduce::new(
            Box::new(ClickhouseWriterStep::new(
                CommitOffsets::new(Duration::from_secs(1)),
                self.storage_config.clickhouse_cluster.clone(),
                self.storage_config.clickhouse_table_name.clone(),
                self.skip_write,
                &clickhouse_concurrency,
            )),
            accumulator,
            BytesInsertBatch::default(),
            self.max_batch_size,
            self.max_batch_time,
        );

        match (
            self.use_rust_processor,
            processors::get_processing_function(
                &self.storage_config.message_processor.python_class_name,
            ),
        ) {
            (true, Some(func)) => {
                let task_runner = MessageProcessor { func };
                Box::new(ValidateSchema::new(
                    RunTaskInThreads::new(
                        next_step,
                        Box::new(task_runner),
                        &self.concurrency,
                        Some("process_message"),
                    ),
                    &self.logical_topic_name,
                    false,
                    &self.concurrency,
                ))
            }
            _ => Box::new(
                PythonTransformStep::new(
                    self.storage_config.message_processor.clone(),
                    self.concurrency.concurrency,
                    self.python_max_queue_depth,
                    next_step,
                )
                .unwrap(),
            ),
        }
    }
}
