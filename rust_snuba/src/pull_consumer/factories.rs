use std::sync::Arc;
use std::time::Duration;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::stream::{BatchStage, DlqHandler, KafkaSource};
use sentry_arroyo::types::{Topic, TopicOrPartition};

use crate::config::{self, BatchSizeCalculation, ProcessorConfig, TopicConfig};
use crate::processors::eap_items::EAPItemRow;
use crate::processors::{get_processing_function, ProcessingFunctionType};
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::producers::DryRunProducer;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::cogs_stage::CogsStage;
use crate::pull::stages::commit_log_stage::CommitLogStage;
use crate::pull::stages::processor_stage::ProcessorStage;
use crate::pull::writer::{ClickHouseWriter, DryRunWriter};
use crate::strategies::clickhouse::writer_v2::{ClickhouseClient, InsertFormat};

// ── Shared resources ──────────────────────────────────────

pub(super) struct SharedResources {
    pub dlq_producer: Arc<dyn Producer<KafkaPayload>>,
    pub commit_log_producer: Arc<dyn Producer<KafkaPayload>>,
    pub cogs_producer: Arc<dyn Producer<KafkaPayload>>,
    pub ch_writer: Arc<dyn ClickHouseWriter>,
}

pub(super) fn make_shared_resources(
    consumer_config: &config::ConsumerConfig,
    storage: &config::StorageConfig,
    processor_config: &ProcessorConfig,
    use_row_binary: bool,
    dry_run: bool,
    dry_run_latency: Option<Duration>,
) -> SharedResources {
    SharedResources {
        dlq_producer: Arc::from(make_dlq_producer(consumer_config, dry_run)),
        commit_log_producer: Arc::from(make_commit_log_producer(consumer_config, dry_run)),
        cogs_producer: Arc::from(make_cogs_producer(consumer_config, dry_run)),
        ch_writer: Arc::from(make_ch_writer(
            storage,
            processor_config,
            use_row_binary,
            dry_run_latency,
        )),
    }
}

// ── Producer factories ────────────────────

fn make_kafka_producer(topic_config: &TopicConfig) -> KafkaProducer {
    KafkaProducer::new(KafkaConfig::new_producer_config(
        vec![],
        Some(topic_config.broker_config.clone()),
    ))
    .expect("failed to create kafka producer")
}

fn make_dlq_producer(
    consumer_config: &config::ConsumerConfig,
    dry_run: bool,
) -> Box<dyn Producer<KafkaPayload>> {
    match consumer_config.dlq_topic.as_ref() {
        Some(tc) if !dry_run => Box::new(make_kafka_producer(tc)),
        _ => Box::new(DryRunProducer),
    }
}

fn make_commit_log_producer(
    consumer_config: &config::ConsumerConfig,
    dry_run: bool,
) -> Box<dyn Producer<KafkaPayload>> {
    match consumer_config.commit_log_topic.as_ref() {
        Some(tc) if !dry_run => Box::new(make_kafka_producer(tc)),
        _ => Box::new(DryRunProducer),
    }
}

fn make_cogs_producer(
    consumer_config: &config::ConsumerConfig,
    dry_run: bool,
) -> Box<dyn Producer<KafkaPayload>> {
    if !dry_run && consumer_config.env.record_cogs {
        Box::new(make_kafka_producer(&consumer_config.accountant_topic))
    } else {
        Box::new(DryRunProducer)
    }
}

fn make_ch_writer(
    storage: &config::StorageConfig,
    processor_config: &ProcessorConfig,
    use_row_binary: bool,
    dry_run_latency: Option<Duration>,
) -> Box<dyn ClickHouseWriter> {
    if let Some(latency) = dry_run_latency {
        return Box::new(DryRunWriter::new(latency));
    }

    if use_row_binary {
        Box::new(ClickhouseClient::new(
            &storage.clickhouse_cluster,
            &storage.clickhouse_table_name,
            storage.name.clone(),
            InsertFormat::RowBinary,
            Some(EAPItemRow::column_names(
                processor_config.eap_items_emit_received_at,
            )),
        ))
    } else {
        Box::new(ClickhouseClient::new(
            &storage.clickhouse_cluster,
            &storage.clickhouse_table_name,
            storage.name.clone(),
            InsertFormat::JsonEachRow,
            None,
        ))
    }
}

// ── Stage factories ───────────────────────────────────────

pub(super) fn resolve_processor(
    processor_name: &str,
) -> Result<crate::processors::ProcessingFunction, String> {
    match get_processing_function(processor_name) {
        Some(ProcessingFunctionType::ProcessingFunction(f)) => Ok(f),
        Some(ProcessingFunctionType::ProcessingFunctionWithReplacements(_)) => Err(format!(
            "{processor_name} is a replacement processor — not supported"
        )),
        None => Err(format!("Unknown processor: {processor_name}")),
    }
}

pub(super) fn make_kafka_source(
    consumer_config: &config::ConsumerConfig,
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    max_poll_interval_ms: usize,
) -> KafkaSource {
    let kafka_config = KafkaConfig::new_consumer_config(
        vec![],
        consumer_group.to_owned(),
        auto_offset_reset
            .parse()
            .expect("Invalid auto_offset_reset"),
        !no_strict_offset_reset,
        max_poll_interval_ms,
        Some(consumer_config.raw_topic.broker_config.clone()),
    );
    let topic = Topic::new(&consumer_config.raw_topic.physical_topic_name);
    KafkaSource::new(kafka_config, &[topic])
}

pub(super) fn make_processor_config(
    storage: &config::StorageConfig,
    env_config: &config::EnvConfig,
) -> ProcessorConfig {
    ProcessorConfig {
        env_config: env_config.clone(),
        storage_name: storage.name.clone(),
        eap_items_emit_received_at: crate::processors::eap_items::emit_received_at(),
    }
}

pub(super) fn make_dlq_handler(
    producer: &Arc<dyn Producer<KafkaPayload>>,
    topic_config: Option<&TopicConfig>,
    dry_run: bool,
) -> DlqHandler {
    let topic_name = match topic_config {
        Some(tc) if !dry_run => tc.physical_topic_name.as_str(),
        _ => "dry-run-dlq",
    };
    DlqHandler::new(
        Arc::clone(producer),
        TopicOrPartition::Topic(Topic::new(topic_name)),
    )
}

pub(super) fn make_commit_log_stage(
    producer: &Arc<dyn Producer<KafkaPayload>>,
    topic_config: Option<&TopicConfig>,
    source_topic: &str,
    consumer_group: &str,
    dry_run: bool,
) -> CommitLogStage {
    let dest_name = match topic_config {
        Some(tc) if !dry_run => tc.physical_topic_name.as_str(),
        _ => "dry-run-commit-log",
    };
    CommitLogStage::new(
        Arc::clone(producer),
        Topic::new(dest_name),
        Topic::new(source_topic),
        consumer_group.to_string(),
    )
}

pub(super) fn make_cogs_stage(
    producer: &Arc<dyn Producer<KafkaPayload>>,
    topic_config: &TopicConfig,
    resource_id: String,
    dry_run: bool,
    record_cogs: bool,
) -> CogsStage {
    let dest_name = if !dry_run && record_cogs {
        topic_config.physical_topic_name.as_str()
    } else {
        "dry-run-cogs"
    };
    CogsStage::new(Arc::clone(producer), Topic::new(dest_name), resource_id)
}

pub(super) fn make_writer_stage(writer: &Arc<dyn ClickHouseWriter>) -> ClickHouseWriterStage {
    ClickHouseWriterStage::new(Arc::clone(writer))
}

pub(super) fn make_processor_stage(
    processor: crate::processors::ProcessingFunction,
    processor_config: &ProcessorConfig,
) -> ProcessorStage {
    ProcessorStage::new(processor, processor_config.clone())
}

pub(super) fn make_batch_stage(
    max_batch_size: u64,
    calculation: BatchSizeCalculation,
) -> BatchStage<PipelineBatch, PipelineBatchBuffer> {
    let (max_rows, max_bytes) = match calculation {
        BatchSizeCalculation::Rows => (max_batch_size, u64::MAX),
        BatchSizeCalculation::Bytes => (u64::MAX, max_batch_size),
    };
    BatchStage::new(PipelineBatchBuffer::new(), max_rows, max_bytes)
}

pub(super) fn make_flush_timers(
    consumer_config: &config::ConsumerConfig,
) -> (Option<Duration>, Option<Duration>) {
    let cadence = Some(Duration::from_millis(consumer_config.max_batch_time_ms));
    let idle_timeout = None;
    (cadence, idle_timeout)
}
