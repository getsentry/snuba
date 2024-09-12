use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use rust_arroyo::processing::strategies::reduce::Reduce;
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskInThreads,
};
use rust_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use rust_arroyo::types::Message;
use rust_arroyo::types::{Partition, Topic};

use crate::config;
use crate::metrics::global_tags::set_global_tag;
use crate::mutations::clickhouse::ClickhouseWriter;
use crate::mutations::parser::{MutationBatch, MutationMessage, MutationParser};

pub struct MutConsumerStrategyFactory {
    pub storage_config: config::StorageConfig,
    pub env_config: config::EnvConfig,
    pub logical_topic_name: String,
    pub max_batch_size: usize,
    pub max_batch_time: Duration,
    pub processing_concurrency: ConcurrencyConfig,
    pub clickhouse_concurrency: ConcurrencyConfig,
    pub async_inserts: bool,
    pub python_max_queue_depth: Option<usize>,
    pub use_rust_processor: bool,
    pub health_check_file: Option<String>,
    pub enforce_schema: bool,
    pub physical_consumer_group: String,
    pub physical_topic_name: Topic,
    pub accountant_topic_config: config::TopicConfig,
    pub batch_write_timeout: Option<Duration>,
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

        let next_step = RunTaskInThreads::new(
            Box::new(next_step),
            Box::new(ClickhouseWriter::new(
                &self.storage_config.clickhouse_cluster.host,
                self.storage_config.clickhouse_cluster.http_port,
                &self.storage_config.clickhouse_table_name,
                &self.storage_config.clickhouse_cluster.database,
                &self.storage_config.clickhouse_cluster.user,
                &self.storage_config.clickhouse_cluster.password,
                self.batch_write_timeout,
            )),
            &self.clickhouse_concurrency,
            Some("clickhouse"),
        );

        let next_step = Reduce::new(
            next_step,
            Arc::new(
                move |mut batch: MutationBatch, message: Message<MutationMessage>| {
                    let message = message.into_payload();
                    match batch.0.entry(message.filter) {
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            entry.get_mut().merge(message.update);
                        }
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(message.update);
                        }
                    }

                    Ok(batch)
                },
            ),
            Arc::new(move || MutationBatch::default()),
            self.max_batch_size,
            self.max_batch_time,
            // TODO: batch sizes are currently not properly computed. if two mutations are merged
            // together, they still count against the batch size as 2.
            |_| 1,
        );

        let next_step = RunTaskInThreads::new(
            Box::new(next_step),
            Box::new(MutationParser),
            &self.processing_concurrency,
            Some("parse"),
        );

        Box::new(next_step)
    }
}
