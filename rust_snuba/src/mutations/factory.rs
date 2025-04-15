use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::healthcheck::HealthCheck;
use sentry_arroyo::processing::strategies::reduce::Reduce;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskInThreads,
};
use sentry_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use sentry_arroyo::types::Message;
use sentry_arroyo::types::Partition;

use crate::config;
use crate::metrics::global_tags::set_global_tag;
use crate::mutations::clickhouse::ClickhouseWriter;
use crate::mutations::parser::{MutationBatch, MutationMessage, MutationParser};
use crate::processors::eap_spans::PrimaryKey;

use crate::mutations::synchronize::Synchronizer;

pub struct MutConsumerStrategyFactory {
    pub storage_config: config::StorageConfig,
    pub max_batch_size: usize,
    pub max_batch_time: Duration,
    pub processing_concurrency: ConcurrencyConfig,
    pub clickhouse_concurrency: ConcurrencyConfig,
    pub health_check_file: Option<String>,
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
            next_step,
            ClickhouseWriter::new(
                &self.storage_config.clickhouse_cluster.host,
                self.storage_config.clickhouse_cluster.http_port,
                &self.storage_config.clickhouse_table_name,
                &self.storage_config.clickhouse_cluster.database,
                &self.storage_config.clickhouse_cluster.user,
                &self.storage_config.clickhouse_cluster.password,
                self.storage_config.clickhouse_cluster.secure,
                self.batch_write_timeout,
            ),
            &self.clickhouse_concurrency,
            Some("clickhouse"),
        );

        let next_step = Reduce::new(
            next_step,
            Arc::new(
                move |mut batch: MutationBatch, message: Message<MutationMessage>| {
                    let message = message.into_payload();
                    let filter: PrimaryKey = message.filter.into();
                    match batch.0.entry(filter) {
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
            Arc::new(MutationBatch::default),
            self.max_batch_size,
            self.max_batch_time,
            // TODO: batch sizes are currently not properly computed. if two mutations are merged
            // together, they still count against the batch size as 2.
            |_| 1,
        );

        let next_step = RunTaskInThreads::new(
            next_step,
            MutationParser,
            &self.processing_concurrency,
            Some("parse"),
        );

        let mut synchronizer = Synchronizer {
            min_delay: TimeDelta::hours(1),
        };

        let next_step = RunTask::new(move |m| synchronizer.process_message(m), next_step);

        if let Some(path) = &self.health_check_file {
            Box::new(HealthCheck::new(next_step, path))
        } else {
            Box::new(next_step)
        }
    }
}
