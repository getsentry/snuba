use std::sync::Arc;
use std::time::Duration;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::healthcheck::HealthCheck;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};

use crate::config;
use crate::processors;
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::processor::make_rust_processor;
use crate::strategies::python_v2::PythonTransformStep;
use crate::types::BytesInsertBatch;

pub struct ConsumerStrategyFactory {
    storage_config: config::StorageConfig,
    logical_topic_name: String,
    max_batch_size: usize,
    max_batch_time: Duration,
    skip_write: bool,
    processing_concurrency: ConcurrencyConfig,
    clickhouse_concurrency: ConcurrencyConfig,
    python_max_queue_depth: Option<usize>,
    use_rust_processor: bool,
    health_check_file: Option<String>,
}

impl ConsumerStrategyFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_config: config::StorageConfig,
        logical_topic_name: String,
        max_batch_size: usize,
        max_batch_time: Duration,
        skip_write: bool,
        processing_concurrency: ConcurrencyConfig,
        clickhouse_concurrency: ConcurrencyConfig,
        python_max_queue_depth: Option<usize>,
        use_rust_processor: bool,
        health_check_file: Option<String>,
    ) -> Self {
        Self {
            storage_config,
            logical_topic_name,
            max_batch_size,
            max_batch_time,
            skip_write,
            processing_concurrency,
            clickhouse_concurrency,
            python_max_queue_depth,
            use_rust_processor,
            health_check_file,
        }
    }
}

impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let accumulator = Arc::new(BytesInsertBatch::merge);
        let compute_batch_length = Some(Arc::new(BytesInsertBatch::len));

        let next_step = Reduce::new(
            Box::new(ClickhouseWriterStep::new(
                CommitOffsets::new(chrono::Duration::seconds(1)),
                self.storage_config.clickhouse_cluster.clone(),
                self.storage_config.clickhouse_table_name.clone(),
                self.skip_write,
                &self.clickhouse_concurrency,
            )),
            accumulator,
            BytesInsertBatch::default(),
            self.max_batch_size,
            self.max_batch_time,
            compute_batch_length,
        );

        let processor = match (
            self.use_rust_processor,
            processors::get_processing_function(
                &self.storage_config.message_processor.python_class_name,
            ),
        ) {
            (true, Some(func)) => make_rust_processor(
                next_step,
                func,
                &self.logical_topic_name,
                false,
                &self.processing_concurrency,
            ),
            _ => Box::new(
                PythonTransformStep::new(
                    next_step,
                    self.storage_config.message_processor.clone(),
                    self.processing_concurrency.concurrency,
                    self.python_max_queue_depth,
                )
                .unwrap(),
            ),
        };

        if let Some(path) = &self.health_check_file {
            Box::new(HealthCheck::new(processor, path))
        } else {
            processor
        }
    }
}
