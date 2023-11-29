use std::sync::Arc;
use std::time::Duration;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};

use crate::config;
use crate::processors;
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::processor::make_rust_processor;
use crate::strategies::python::PythonTransformStep;
use crate::types::BytesInsertBatch;

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
            (true, Some(func)) => make_rust_processor(
                next_step,
                func,
                &self.logical_topic_name,
                false,
                &self.concurrency,
            ),
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
