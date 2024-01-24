use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::healthcheck::HealthCheck;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskInThreads,
};
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::types::Message;
use rust_arroyo::types::{Partition, Topic};
use sentry::{Hub, SentryFutureExt};
use sentry_kafka_schemas::Schema;

use crate::config;
use crate::metrics::global_tags::set_global_tag;
use crate::processors::{self, get_cogs_label};
use crate::strategies::accountant::RecordCogs;
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::commit_log::ProduceCommitLog;
use crate::strategies::processor::{
    get_schema, make_rust_processor, make_rust_processor_with_replacements, validate_schema,
};
use crate::strategies::python::PythonTransformStep;
use crate::strategies::replacements::ProduceReplacements;
use crate::types::BytesInsertBatch;

pub struct ConsumerStrategyFactory {
    storage_config: config::StorageConfig,
    env_config: config::EnvConfig,
    logical_topic_name: String,
    max_batch_size: usize,
    max_batch_time: Duration,
    skip_write: bool,
    processing_concurrency: ConcurrencyConfig,
    clickhouse_concurrency: ConcurrencyConfig,
    commitlog_concurrency: ConcurrencyConfig,
    replacements_concurrency: ConcurrencyConfig,
    python_max_queue_depth: Option<usize>,
    use_rust_processor: bool,
    health_check_file: Option<String>,
    enforce_schema: bool,
    commit_log_producer: Option<(Arc<KafkaProducer>, Topic)>,
    physical_consumer_group: String,
    physical_topic_name: Topic,
    accountant_topic_config: config::TopicConfig,
}

impl ConsumerStrategyFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_config: config::StorageConfig,
        env_config: config::EnvConfig,
        logical_topic_name: String,
        max_batch_size: usize,
        max_batch_time: Duration,
        skip_write: bool,
        processing_concurrency: ConcurrencyConfig,
        clickhouse_concurrency: ConcurrencyConfig,
        commitlog_concurrency: ConcurrencyConfig,
        replacements_concurrency: ConcurrencyConfig,
        python_max_queue_depth: Option<usize>,
        use_rust_processor: bool,
        health_check_file: Option<String>,
        enforce_schema: bool,
        commit_log_producer: Option<(Arc<KafkaProducer>, Topic)>,
        physical_consumer_group: String,
        physical_topic_name: Topic,
        accountant_topic_config: config::TopicConfig,
    ) -> Self {
        Self {
            storage_config,
            env_config,
            logical_topic_name,
            max_batch_size,
            max_batch_time,
            skip_write,
            processing_concurrency,
            clickhouse_concurrency,
            commitlog_concurrency,
            replacements_concurrency,
            python_max_queue_depth,
            use_rust_processor,
            health_check_file,
            enforce_schema,
            commit_log_producer,
            physical_consumer_group,
            physical_topic_name,
            accountant_topic_config,
        }
    }
}

impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
    fn update_partitions(&self, partitions: &HashMap<Partition, u64>) {
        match partitions.keys().map(|partition| partition.index).min() {
            Some(min) => set_global_tag("min_partition".to_owned(), min.to_string()),
            None => set_global_tag("min_partition".to_owned(), "none".to_owned()),
        }
    }

    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        // Commit offsets
        let next_step = CommitOffsets::new(chrono::Duration::seconds(1));

        // Produce commit log if there is one
        let next_step: Box<dyn ProcessingStrategy<_>> =
            if let Some((ref producer, destination)) = self.commit_log_producer {
                Box::new(ProduceCommitLog::new(
                    next_step,
                    producer.clone(),
                    destination,
                    self.physical_topic_name,
                    self.physical_consumer_group.clone(),
                    &self.commitlog_concurrency,
                    self.skip_write,
                ))
            } else {
                Box::new(next_step)
            };

        // Write to clickhouse
        let next_step = Box::new(ClickhouseWriterStep::new(
            next_step,
            self.storage_config.clickhouse_cluster.clone(),
            self.storage_config.clickhouse_table_name.clone(),
            self.skip_write,
            &self.clickhouse_concurrency,
        ));

        let cogs_label = get_cogs_label(&self.storage_config.message_processor.python_class_name);

        // Produce cogs if generic metrics AND we are not skipping writes AND record_cogs is true
        let next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>> =
            match (self.skip_write, self.env_config.record_cogs, cogs_label) {
                (false, true, Some(resource_id)) => Box::new(RecordCogs::new(
                    next_step,
                    resource_id,
                    self.accountant_topic_config.broker_config.clone(),
                    &self.accountant_topic_config.physical_topic_name,
                )),
                _ => next_step,
            };

        // Batch insert rows
        let accumulator = Arc::new(BytesInsertBatch::merge);
        let next_step = Reduce::new(
            next_step,
            accumulator,
            BytesInsertBatch::default(),
            self.max_batch_size,
            self.max_batch_time,
            BytesInsertBatch::len,
            // we need to enable this to deal with storages where we skip 100% of values, such as
            // gen-metrics-gauges in s4s. we still need to commit there
        )
        .flush_empty_batches(true);

        // Transform messages
        let processor = match (
            self.use_rust_processor,
            processors::get_processing_function(
                &self.storage_config.message_processor.python_class_name,
            ),
            processors::get_processing_function_with_replacements(
                &self.storage_config.message_processor.python_class_name,
            ),
        ) {
            (true, _, Some(func)) => {
                // TODO: Replacements configuration
                let replacements_producer: Option<KafkaProducer> = None;
                let replacements_destination = None;
                let replacements_step = ProduceReplacements::new(
                    next_step,
                    replacements_producer,
                    replacements_destination,
                    &self.replacements_concurrency,
                    self.skip_write,
                );

                return make_rust_processor_with_replacements(
                    replacements_step,
                    func,
                    &self.logical_topic_name,
                    self.enforce_schema,
                    &self.processing_concurrency,
                    config::ProcessorConfig {
                        env_config: self.env_config.clone(),
                    },
                );
            }
            (true, Some(func), _) => make_rust_processor(
                next_step,
                func,
                &self.logical_topic_name,
                self.enforce_schema,
                &self.processing_concurrency,
                config::ProcessorConfig {
                    env_config: self.env_config.clone(),
                },
            ),
            _ => {
                let schema = get_schema(&self.logical_topic_name, self.enforce_schema);

                Box::new(RunTaskInThreads::new(
                    Box::new(
                        PythonTransformStep::new(
                            next_step,
                            self.storage_config.message_processor.clone(),
                            self.processing_concurrency.concurrency,
                            self.python_max_queue_depth,
                        )
                        .unwrap(),
                    ),
                    Box::new(SchemaValidator {
                        schema,
                        enforce_schema: self.enforce_schema,
                    }),
                    &self.processing_concurrency,
                    Some("validate_schema"),
                ))
            }
        };

        if let Some(path) = &self.health_check_file {
            Box::new(HealthCheck::new(processor, path))
        } else {
            processor
        }
    }
}

#[derive(Clone)]
struct SchemaValidator {
    schema: Option<Arc<Schema>>,
    enforce_schema: bool,
}

impl SchemaValidator {
    async fn process_message(
        self,
        message: Message<KafkaPayload>,
    ) -> Result<Message<KafkaPayload>, RunTaskError<anyhow::Error>> {
        validate_schema(&message, &self.schema, self.enforce_schema)?;
        Ok(message)
    }
}

impl TaskRunner<KafkaPayload, KafkaPayload, anyhow::Error> for SchemaValidator {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<KafkaPayload, anyhow::Error> {
        Box::pin(
            self.clone()
                .process_message(message)
                .bind_hub(Hub::new_from_top(Hub::current())),
        )
    }
}
