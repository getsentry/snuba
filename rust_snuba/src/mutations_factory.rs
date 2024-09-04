use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
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
use crate::strategies::processor::validate_schema;

pub struct MutConsumerStrategyFactory {
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
    pub max_bytes_before_external_group_by: Option<usize>,
}

impl ProcessingStrategyFactory<KafkaPayload> for MutConsumerStrategyFactory {
    fn update_partitions(&self, partitions: &HashMap<Partition, u64>) {
        match partitions.keys().map(|partition| partition.index).min() {
            Some(min) => set_global_tag("min_partition".to_owned(), min.to_string()),
            None => set_global_tag("min_partition".to_owned(), "none".to_owned()),
        }
    }

    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let next_step = CommitOffsets::new(Duration::from_secs(1));

        Box::new(next_step)
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
