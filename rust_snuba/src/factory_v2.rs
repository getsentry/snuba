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
use crate::strategies::clickhouse::row_binary_writer::ClickhouseRowBinaryWriterStep;
use crate::strategies::clickhouse::writer_v2::ClickhouseWriterStep;
use crate::strategies::commit_log::ProduceCommitLog;
use crate::strategies::healthcheck::HealthCheck as SnubaHealthCheck;
use crate::strategies::join_timeout::SetJoinTimeout;
use crate::strategies::processor::{
    get_schema, make_rust_processor, make_rust_processor_row_binary,
    make_rust_processor_with_replacements, validate_schema,
};
use crate::strategies::python::PythonTransformStep;
use crate::strategies::replacements::ProduceReplacements;
use crate::types::{BytesInsertBatch, CogsData, RowData, TypedInsertBatch};

pub struct ConsumerStrategyFactoryV2 {
    pub storage_config: config::StorageConfig,
    pub env_config: config::EnvConfig,
    pub logical_topic_name: String,
    pub max_batch_size: usize,
    pub max_batch_time: Duration,
    pub max_batch_size_calculation: config::BatchSizeCalculation,
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
    pub join_timeout_ms: Option<u64>,
    pub health_check: String,
    pub use_row_binary: bool,
}

impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactoryV2 {
    fn update_partitions(&self, partitions: &HashMap<Partition, u64>) {
        let mut assigned_partitions = partitions
            .keys()
            .map(|partition| partition.index)
            .collect::<Vec<_>>();
        assigned_partitions.sort_unstable();
        let assigned_partitions_string = assigned_partitions
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<String>>()
            .join("-");
        set_global_tag("assigned_partitions".to_owned(), assigned_partitions_string);

        match partitions.keys().map(|partition| partition.index).min() {
            Some(min) => set_global_tag("min_partition".to_owned(), min.to_string()),
            None => set_global_tag("min_partition".to_owned(), "none".to_owned()),
        }
    }

    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        if self.use_row_binary {
            return match self
                .storage_config
                .message_processor
                .python_class_name
                .as_str()
            {
                "EAPItemsProcessor" => self.create_row_binary_pipeline(
                    crate::processors::eap_items::process_message_row_binary,
                ),
                name => panic!("RowBinary not supported for processor: {name}"),
            };
        }

        // ---- Existing JSONEachRow path (unchanged below this line) ----

        // Commit offsets
        let next_step = CommitOffsets::new(Duration::from_secs(1));

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
                _ => Box::new(next_step),
            };

        let next_step = SetJoinTimeout::new(
            next_step,
            Some(Duration::from_millis(self.join_timeout_ms.unwrap_or(0))),
        );

        let next_step = ClickhouseWriterStep::new(
            next_step,
            self.storage_config.clickhouse_cluster.clone(),
            self.storage_config.clickhouse_table_name.clone(),
            false,
            &self.clickhouse_concurrency,
            self.storage_config.name.clone(),
        );

        let accumulator = Arc::new(
            |batch: BytesInsertBatch<RowData>, small_batch: Message<BytesInsertBatch<RowData>>| {
                Ok(batch.merge(small_batch.into_payload()))
            },
        );

        let compute_batch_size: fn(&BytesInsertBatch<RowData>) -> usize =
            match self.max_batch_size_calculation {
                config::BatchSizeCalculation::Bytes => |batch| batch.num_bytes(),
                config::BatchSizeCalculation::Rows => |batch| batch.len(),
            };

        let next_step = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || {
                BytesInsertBatch::<RowData>::new(
                    RowData::default(),
                    None,
                    None,
                    None,
                    Default::default(),
                    CogsData::default(),
                )
            }),
            self.max_batch_size,
            self.max_batch_time,
            compute_batch_size,
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
        let next_step = SetJoinTimeout::new(
            next_step,
            Some(Duration::from_millis(self.join_timeout_ms.unwrap_or(0))),
        );
        if let Some(path) = &self.health_check_file {
            {
                if self.health_check == "snuba" {
                    tracing::info!(
                        "Using Snuba HealthCheck for consumer group: {}",
                        self.physical_consumer_group
                    );
                    Box::new(SnubaHealthCheck::new(next_step, path))
                } else {
                    Box::new(HealthCheck::new(next_step, path))
                }
            }
        } else {
            Box::new(next_step)
        }
    }
}

impl ConsumerStrategyFactoryV2 {
    fn create_row_binary_pipeline<
        T: clickhouse::Row
            + serde::Serialize
            + Clone
            + Send
            + Sync
            + crate::types::EstimatedSize
            + 'static,
    >(
        &self,
        func: fn(
            KafkaPayload,
            crate::types::KafkaMessageMetadata,
            &config::ProcessorConfig,
        ) -> anyhow::Result<TypedInsertBatch<T>>,
    ) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        // Commit offsets
        let next_step = CommitOffsets::new(Duration::from_secs(1));

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

        let next_step: Box<dyn ProcessingStrategy<BytesInsertBatch<()>>> =
            match (self.env_config.record_cogs, cogs_label) {
                (true, Some(resource_id)) => Box::new(RecordCogs::new(
                    next_step,
                    resource_id,
                    self.accountant_topic_config.broker_config.clone(),
                    &self.accountant_topic_config.physical_topic_name,
                )),
                _ => Box::new(next_step),
            };

        let next_step = SetJoinTimeout::new(
            next_step,
            Some(Duration::from_millis(self.join_timeout_ms.unwrap_or(0))),
        );

        let next_step = ClickhouseRowBinaryWriterStep::<T, _>::new(
            next_step,
            self.storage_config.clickhouse_cluster.clone(),
            self.storage_config.clickhouse_table_name.clone(),
            &self.clickhouse_concurrency,
            self.storage_config.name.clone(),
        );

        let accumulator = Arc::new(
            |batch: BytesInsertBatch<Vec<T>>, small_batch: Message<BytesInsertBatch<Vec<T>>>| {
                Ok(batch.merge(small_batch.into_payload()))
            },
        );

        type BatchSizeFn<T> = fn(&BytesInsertBatch<Vec<T>>) -> usize;
        let compute_batch_size: BatchSizeFn<T> = match self.max_batch_size_calculation {
            config::BatchSizeCalculation::Bytes => |batch| batch.num_bytes(),
            config::BatchSizeCalculation::Rows => |batch| batch.len(),
        };

        let next_step = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || {
                BytesInsertBatch::<Vec<T>>::new(
                    Vec::new(),
                    None,
                    None,
                    None,
                    Default::default(),
                    CogsData::default(),
                )
            }),
            self.max_batch_size,
            self.max_batch_time,
            compute_batch_size,
        )
        .flush_empty_batches(true);

        let next_step = make_rust_processor_row_binary(
            next_step,
            func,
            &self.logical_topic_name,
            self.enforce_schema,
            &self.processing_concurrency,
            config::ProcessorConfig {
                env_config: self.env_config.clone(),
            },
            self.stop_at_timestamp,
        );

        let next_step = SetJoinTimeout::new(
            next_step,
            Some(Duration::from_millis(self.join_timeout_ms.unwrap_or(0))),
        );

        if let Some(path) = &self.health_check_file {
            if self.health_check == "snuba" {
                tracing::info!(
                    "Using Snuba HealthCheck for consumer group: {}",
                    self.physical_consumer_group
                );
                Box::new(SnubaHealthCheck::new(next_step, path))
            } else {
                Box::new(HealthCheck::new(next_step, path))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use sentry_arroyo::processing::strategies::{
        CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
    };
    use sentry_arroyo::types::{BrokerMessage, InnerMessage, Partition, Topic};
    use std::sync::{Arc, Mutex};

    /// A next-step that records every batch it receives.
    struct RecordingStep {
        batches: Arc<Mutex<Vec<BytesInsertBatch<RowData>>>>,
    }

    impl ProcessingStrategy<BytesInsertBatch<RowData>> for RecordingStep {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }

        fn submit(
            &mut self,
            message: Message<BytesInsertBatch<RowData>>,
        ) -> Result<(), SubmitError<BytesInsertBatch<RowData>>> {
            self.batches.lock().unwrap().push(message.into_payload());
            Ok(())
        }

        fn terminate(&mut self) {}

        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    /// Create a BytesInsertBatch<RowData> with a specific number of rows and byte size.
    fn make_batch(num_rows: usize, num_bytes: usize) -> BytesInsertBatch<RowData> {
        BytesInsertBatch::<RowData>::new(
            RowData {
                encoded_rows: vec![0u8; num_bytes],
                num_rows,
            },
            None,
            None,
            None,
            Default::default(),
            CogsData::default(),
        )
        .with_num_bytes(num_bytes)
    }

    fn make_message(
        batch: BytesInsertBatch<RowData>,
        partition: Partition,
        offset: u64,
    ) -> Message<BytesInsertBatch<RowData>> {
        Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                batch,
                partition,
                offset,
                chrono::Utc::now(),
            )),
        }
    }

    fn build_reduce(
        next_step: RecordingStep,
        max_batch_size: usize,
        calculation: config::BatchSizeCalculation,
    ) -> Reduce<BytesInsertBatch<RowData>, BytesInsertBatch<RowData>> {
        let accumulator = Arc::new(
            |batch: BytesInsertBatch<RowData>, small_batch: Message<BytesInsertBatch<RowData>>| {
                Ok(batch.merge(small_batch.into_payload()))
            },
        );

        let compute_batch_size: fn(&BytesInsertBatch<RowData>) -> usize = match calculation {
            config::BatchSizeCalculation::Bytes => |batch| batch.num_bytes(),
            config::BatchSizeCalculation::Rows => |batch| batch.len(),
        };

        Reduce::new(
            next_step,
            accumulator,
            Arc::new(|| {
                BytesInsertBatch::<RowData>::new(
                    RowData::default(),
                    None,
                    None,
                    None,
                    Default::default(),
                    CogsData::default(),
                )
            }),
            max_batch_size,
            // Very long timeout so only size triggers flush
            Duration::from_secs(3600),
            compute_batch_size,
        )
        .flush_empty_batches(true)
    }

    /// With row-based calculation: 20 messages with max_batch_size=10 should produce
    /// 2 batches (flushed on size) + 1 final batch (flushed on join).
    /// Each batch produced on size should have exactly 10 rows.
    #[test]
    fn test_row_based_batching() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: batches.clone(),
        };

        let mut strategy = build_reduce(next_step, 10, config::BatchSizeCalculation::Rows);

        let partition = Partition::new(Topic::new("test"), 0);

        // Submit 20 messages, each with 1 row and 10_000 bytes
        for i in 0..20 {
            let batch = make_batch(1, 10_000);
            strategy.submit(make_message(batch, partition, i)).unwrap();
            let _ = strategy.poll();
        }

        let _ = strategy.join(None);

        let batches = batches.lock().unwrap();
        // 2 full batches of 10 (flushed on size) + 1 empty batch (flushed on join
        // because flush_empty_batches is enabled)
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 10);
        assert_eq!(batches[1].len(), 10);
        // Total bytes per full batch: 10 messages * 10_000 bytes = 100_000
        assert_eq!(batches[0].num_bytes(), 100_000);
        assert_eq!(batches[1].num_bytes(), 100_000);
    }

    /// With byte-based calculation: same 20 messages (each 10KB), but max_batch_size=50_000
    /// (50KB). Should flush every 5 messages (5 * 10KB = 50KB), producing 4 batches on size
    /// + 1 final batch on join.
    #[test]
    fn test_byte_based_batching() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: batches.clone(),
        };

        let mut strategy = build_reduce(next_step, 50_000, config::BatchSizeCalculation::Bytes);

        let partition = Partition::new(Topic::new("test"), 0);

        // Submit 20 messages, each with 1 row and 10_000 bytes
        for i in 0..20 {
            let batch = make_batch(1, 10_000);
            strategy.submit(make_message(batch, partition, i)).unwrap();
            let _ = strategy.poll();
        }

        let _ = strategy.join(None);

        let batches = batches.lock().unwrap();
        // 4 full batches of 5 messages (flushed on size) + 1 empty batch (flushed on join)
        assert_eq!(batches.len(), 5);
        // Each full batch: 5 rows, 50_000 bytes
        for batch in batches[..4].iter() {
            assert_eq!(batch.len(), 5);
            assert_eq!(batch.num_bytes(), 50_000);
        }
    }

    /// With byte-based calculation and variable-size messages:
    /// 3 messages of 40KB each with max_batch_size=100_000 bytes.
    /// First 2 messages = 80KB (under limit), 3rd pushes to 120KB (over limit),
    /// so the batch flushes after the 3rd message with all 3 rows.
    /// This confirms that large messages cause earlier flushes than row counting would.
    #[test]
    fn test_byte_based_batching_large_messages_flush_early() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: batches.clone(),
        };

        // 100KB byte limit — with row counting this would allow 100 messages
        let mut strategy = build_reduce(next_step, 100_000, config::BatchSizeCalculation::Bytes);

        let partition = Partition::new(Topic::new("test"), 0);

        // Submit 5 messages of 40KB each
        for i in 0..5 {
            let batch = make_batch(1, 40_000);
            strategy.submit(make_message(batch, partition, i)).unwrap();
            let _ = strategy.poll();
        }

        let _ = strategy.join(None);

        let batches = batches.lock().unwrap();
        // 3 messages hit 120KB >= 100KB limit → flush
        // Then 2 remaining → flush on join
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 3); // 3 * 40KB = 120KB >= 100KB
        assert_eq!(batches[0].num_bytes(), 120_000);
        assert_eq!(batches[1].len(), 2); // 2 * 40KB = 80KB, flushed on join
        assert_eq!(batches[1].num_bytes(), 80_000);
    }

    /// Verify that with row-based calculation, the same large messages do NOT
    /// cause early flushing — all 5 fit in one batch since max_batch_size=100 rows.
    #[test]
    fn test_row_based_batching_ignores_byte_size() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: batches.clone(),
        };

        // Row limit of 100 — 5 messages won't hit this
        let mut strategy = build_reduce(next_step, 100, config::BatchSizeCalculation::Rows);

        let partition = Partition::new(Topic::new("test"), 0);

        // Submit 5 messages of 40KB each — huge bytes but only 5 rows
        for i in 0..5 {
            let batch = make_batch(1, 40_000);
            strategy.submit(make_message(batch, partition, i)).unwrap();
            let _ = strategy.poll();
        }

        let _ = strategy.join(None);

        let batches = batches.lock().unwrap();
        // All 5 fit in one batch (5 rows << 100 row limit), flushed on join
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 5);
        assert_eq!(batches[0].num_bytes(), 200_000); // 5 * 40KB accumulated but didn't trigger flush
    }
}
