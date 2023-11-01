use crate::config;
use crate::processors;
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::python::PythonTransformStep;
use crate::types::{BadMessage, BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::InvalidMessage;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::types::{BrokerMessage, InnerMessage, Message};
use std::sync::Arc;
use std::time::Duration;

pub struct ConsumerStrategyFactory {
    storage_config: config::StorageConfig,
    max_batch_size: usize,
    max_batch_time: Duration,
    skip_write: bool,
    concurrency: usize,
    use_rust_processor: bool,
}

impl ConsumerStrategyFactory {
    pub fn new(
        storage_config: config::StorageConfig,
        max_batch_size: usize,
        max_batch_time: Duration,
        skip_write: bool,
        concurrency: usize,
        use_rust_processor: bool,
    ) -> Self {
        Self {
            storage_config,
            max_batch_size,
            max_batch_time,
            skip_write,
            concurrency,
            use_rust_processor,
        }
    }
}

impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let accumulator = Arc::new(|mut acc: BytesInsertBatch, value: BytesInsertBatch| {
            for row in value.rows {
                acc.rows.push(row);
            }
            acc
        });

        let next_step = Reduce::new(
            Box::new(ClickhouseWriterStep::new(
                CommitOffsets::new(Duration::from_secs(1)),
                self.storage_config.clickhouse_cluster.clone(),
                self.storage_config.clickhouse_table_name.clone(),
                self.skip_write,
                2,
            )),
            accumulator,
            BytesInsertBatch { rows: vec![] },
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
                struct MessageProcessor {
                    func: fn(
                        KafkaPayload,
                        KafkaMessageMetadata,
                    ) -> Result<BytesInsertBatch, BadMessage>,
                }

                impl TaskRunner<KafkaPayload, BytesInsertBatch> for MessageProcessor {
                    fn get_task(
                        &self,
                        message: Message<KafkaPayload>,
                    ) -> RunTaskFunc<BytesInsertBatch> {
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
                                        payload: transformed,
                                        partition: broker_message.partition,
                                        offset: broker_message.offset,
                                        timestamp: broker_message.timestamp,
                                    }),
                                }),
                                Err(_e) => Err(InvalidMessage {
                                    partition: broker_message.partition,
                                    offset: broker_message.offset,
                                }),
                            }
                        })
                    }
                }

                let task_runner = MessageProcessor { func };
                Box::new(RunTaskInThreads::new(
                    next_step,
                    Box::new(task_runner),
                    self.concurrency,
                    Some("process_message"),
                ))
            }
            _ => Box::new(
                PythonTransformStep::new(
                    self.storage_config.message_processor.clone(),
                    self.concurrency,
                    next_step,
                )
                .unwrap(),
            ),
        }
    }
}
