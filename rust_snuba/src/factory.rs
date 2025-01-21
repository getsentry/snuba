use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sentry::{Hub, SentryFutureExt};
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::healthcheck::HealthCheck;
use sentry_arroyo::processing::strategies::reduce::Reduce;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskInThreads,
};
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use sentry_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use sentry_arroyo::types::Message;
use sentry_arroyo::types::{Partition, Topic};
use sentry_kafka_schemas::Schema;

use crate::config;
use crate::metrics::global_tags::set_global_tag;
use crate::processors::{self, get_cogs_label};
use crate::strategies::accountant::RecordCogs;
use crate::strategies::clickhouse::batch::{BatchFactory, HttpBatch};
use crate::strategies::clickhouse::ClickhouseWriterStep;
use crate::strategies::commit_log::ProduceCommitLog;
use crate::strategies::join_timeout::SetJoinTimeout;
use crate::strategies::processor::{
    get_schema, make_rust_processor, make_rust_processor_with_replacements, validate_schema,
};
use crate::strategies::python::PythonTransformStep;
use crate::strategies::replacements::ProduceReplacements;
use crate::types::{BytesInsertBatch, CogsData, RowData};

pub struct ConsumerStrategyFactory {
    pub storage_config: config::StorageConfig,
    pub env_config: config::EnvConfig,
    pub logical_topic_name: String,
    pub max_batch_size: usize,
    pub max_batch_time: Duration,
    pub processing_concurrency: ConcurrencyConfig,
    pub clickhouse_concurrency: ConcurrencyConfig,
    pub commitlog_concurrency: ConcurrencyConfig,
    pub replacements_concurrency: ConcurrencyConfig,
    pub async_inserts: bool,
    pub python_max_queue_depth: Option<usize>,
    pub use_rust_processor: bool,
    pub health_check_file: Option<String>,
    pub enforce_schema: bool,
    pub commit_log_producer: Option<(Arc<KafkaProducer>, Topic)>,
    pub replacements_config: Option<(KafkaConfig, Topic)>,
    pub physical_consumer_group: String,
    pub physical_topic_name: Topic,
    pub accountant_topic_config: config::TopicConfig,
    pub stop_at_timestamp: Option<i64>,
    pub batch_write_timeout: Option<Duration>,
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
        let next_step = CommitOffsets::new(Duration::from_secs(1));

        // Produce commit log if there is one
        let next_step: Box<dyn ProcessingStrategy<BytesInsertBatch<()>>> =
            if let Some((ref producer, destination)) = self.commit_log_producer {
                Box::new(ProduceCommitLog::new(
                    next_step,
                    producer.clone(),
                    destination,
                    self.physical_topic_name,
                    self.physical_consumer_group.clone(),
                    &self.commitlog_concurrency,
                    false,
                ))
            } else {
                Box::new(next_step)
            };

        let cogs_label = get_cogs_label(&self.storage_config.message_processor.python_class_name);

        // Produce cogs if generic metrics AND we are not skipping writes AND record_cogs is true
        let next_step: Box<dyn ProcessingStrategy<BytesInsertBatch<()>>> =
            match (self.env_config.record_cogs, cogs_label) {
                (true, Some(resource_id)) => Box::new(RecordCogs::new(
                    next_step,
                    resource_id,
                    self.accountant_topic_config.broker_config.clone(),
                    &self.accountant_topic_config.physical_topic_name,
                )),
                _ => next_step,
            };

        // Write to clickhouse
        let next_step = ClickhouseWriterStep::new(next_step, &self.clickhouse_concurrency);

        let next_step = SetJoinTimeout::new(next_step, None);

        // Batch insert rows
        let batch_factory = BatchFactory::new(
            &self.storage_config.clickhouse_cluster.host,
            self.storage_config.clickhouse_cluster.http_port,
            &self.storage_config.clickhouse_table_name,
            &self.storage_config.clickhouse_cluster.database,
            &self.clickhouse_concurrency,
            &self.storage_config.clickhouse_cluster.user,
            &self.storage_config.clickhouse_cluster.password,
            self.async_inserts,
            self.batch_write_timeout,
        );

        let accumulator = Arc::new(
            |batch: BytesInsertBatch<HttpBatch>,
             small_batch: Message<BytesInsertBatch<RowData>>| {
                Ok(batch.merge(small_batch.into_payload()))
            },
        );

        let next_step = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || {
                BytesInsertBatch::new(
                    batch_factory.new_batch(),
                    None,
                    None,
                    None,
                    Default::default(),
                    CogsData::default(),
                )
            }),
            self.max_batch_size,
            self.max_batch_time,
            BytesInsertBatch::len,
            // we need to enable this to deal with storages where we skip 100% of values, such as
            // gen-metrics-gauges in s4s. we still need to commit there
        )
        .flush_empty_batches(true);

        // Transform messages
        let next_step = match (
            self.use_rust_processor,
            processors::get_processing_function(
                &self.storage_config.message_processor.python_class_name,
            ),
        ) {
            (
                true,
                Some(processors::ProcessingFunctionType::ProcessingFunctionWithReplacements(func)),
            ) => {
                let (replacements_config, replacements_destination) =
                    self.replacements_config.clone().unwrap();

                let producer = KafkaProducer::new(replacements_config);
                let replacements_step = ProduceReplacements::new(
                    next_step,
                    producer,
                    replacements_destination,
                    &self.replacements_concurrency,
                    false,
                );

                make_rust_processor_with_replacements(
                    replacements_step,
                    func,
                    &self.logical_topic_name,
                    self.enforce_schema,
                    &self.processing_concurrency,
                    config::ProcessorConfig {
                        env_config: self.env_config.clone(),
                    },
                    self.stop_at_timestamp,
                )
            }
            (true, Some(processors::ProcessingFunctionType::ProcessingFunction(func))) => {
                make_rust_processor(
                    next_step,
                    func,
                    &self.logical_topic_name,
                    self.enforce_schema,
                    &self.processing_concurrency,
                    config::ProcessorConfig {
                        env_config: self.env_config.clone(),
                    },
                    self.stop_at_timestamp,
                )
            }
            (
                false,
                Some(processors::ProcessingFunctionType::ProcessingFunctionWithReplacements(_)),
            ) => {
                panic!("Consumer with replacements cannot be run in hybrid-mode");
            }
            _ => {
                let schema = get_schema(&self.logical_topic_name, self.enforce_schema);

                Box::new(RunTaskInThreads::new(
                    PythonTransformStep::new(
                        next_step,
                        self.storage_config.message_processor.clone(),
                        self.processing_concurrency.concurrency,
                        self.python_max_queue_depth,
                    )
                    .unwrap(),
                    SchemaValidator {
                        schema,
                        enforce_schema: self.enforce_schema,
                    },
                    &self.processing_concurrency,
                    Some("validate_schema"),
                ))
            }
        };

        // force message processor to drop all in-flight messages, as it is not worth the time
        // spent in rebalancing to wait for them and it is idempotent anyway. later, we overwrite
        // the timeout again for the clickhouse writer step
        let next_step = SetJoinTimeout::new(next_step, Some(Duration::from_secs(0)));

        if let Some(path) = &self.health_check_file {
            Box::new(HealthCheck::new(next_step, path))
        } else {
            Box::new(next_step)
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
