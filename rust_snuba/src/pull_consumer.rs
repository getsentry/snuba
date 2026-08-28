use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::metrics;
use sentry_arroyo::processing::stream::{
    BatchStage, DlqHandler, KafkaSource, OffsetTracker, Pipeline, PipelineExit, PipelineExt,
    PullSource,
};
use sentry_arroyo::types::{Topic, TopicOrPartition};

use crate::config::{self, BatchSizeCalculation, ProcessorConfig, TopicConfig};
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::statsd::create_dogstatsd_backend;
use crate::processors::eap_items::EAPItemRow;
use crate::processors::{get_cogs_label, get_processing_function, ProcessingFunctionType};
use crate::pull::batch::batch_metadata::BatchMetadata;
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::pipelines::eap::EapPipeline;
use crate::pull::pipelines::fire_and_forget::FireAndForgetPipeline;
use crate::pull::producers::DryRunProducer;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::cogs_stage::CogsStage;
use crate::pull::stages::commit_log_stage::CommitLogStage;
use crate::pull::stages::processor_stage::ProcessorStage;
use crate::pull::writer::{ClickHouseWriter, DryRunWriter};
use crate::strategies::clickhouse::writer_v2::{ClickhouseClient, InsertFormat};

/// Allowed processors for the fire-and-forget pipeline.
const FIRE_AND_FORGET_PROCESSORS: &[&str] = &[
    "FunctionsMessageProcessor",
    "ProfilesMessageProcessor",
    "QuerylogProcessor",
    "ReplaysProcessor",
    "OutcomesProcessor",
    "ProfileChunksProcessor",
    "LlmProxyCostProcessor",
];

/// Allowed processors for the EAP pipeline.
const EAP_PROCESSORS: &[&str] = &["EAPItemsProcessor"];

// ── Shared resources ──────────────────────────────────────

struct SharedResources {
    dlq_producer: Arc<dyn Producer<KafkaPayload>>,
    commit_log_producer: Arc<dyn Producer<KafkaPayload>>,
    cogs_producer: Arc<dyn Producer<KafkaPayload>>,
    ch_writer: Arc<dyn ClickHouseWriter>,
}

fn make_shared_resources(
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

fn resolve_processor(
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

fn make_kafka_source(
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

fn make_processor_config(
    storage: &config::StorageConfig,
    env_config: &config::EnvConfig,
) -> ProcessorConfig {
    ProcessorConfig {
        env_config: env_config.clone(),
        storage_name: storage.name.clone(),
        eap_items_emit_received_at: crate::processors::eap_items::emit_received_at(),
    }
}

fn make_dlq_handler(
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

fn make_commit_log_stage(
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

fn make_cogs_stage(
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

fn make_writer_stage(writer: &Arc<dyn ClickHouseWriter>) -> ClickHouseWriterStage {
    ClickHouseWriterStage::new(Arc::clone(writer))
}

fn make_processor_stage(
    processor: crate::processors::ProcessingFunction,
    processor_config: &ProcessorConfig,
) -> ProcessorStage {
    ProcessorStage::new(processor, processor_config.clone())
}

fn make_batch_stage(
    max_batch_size: u64,
    calculation: BatchSizeCalculation,
) -> BatchStage<PipelineBatch, PipelineBatchBuffer> {
    let (max_rows, max_bytes) = match calculation {
        BatchSizeCalculation::Rows => (max_batch_size, u64::MAX),
        BatchSizeCalculation::Bytes => (u64::MAX, max_batch_size),
    };
    BatchStage::new(PipelineBatchBuffer::new(), max_rows, max_bytes)
}

fn make_flush_timers(
    consumer_config: &config::ConsumerConfig,
) -> (Option<Duration>, Option<Duration>) {
    let cadence = Some(Duration::from_millis(consumer_config.max_batch_time_ms));
    let idle_timeout = None;
    (cadence, idle_timeout)
}

// ── Pipeline assembly ──────────────────────────────────────

fn make_eap_pipeline(
    shared: &SharedResources,
    consumer_config: &config::ConsumerConfig,
    storage: &config::StorageConfig,
    processor_config: &ProcessorConfig,
    consumer_group: &str,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
    dry_run: bool,
) -> EapPipeline {
    let processor_name = &storage.message_processor.python_class_name;
    let source_topic_name = &consumer_config.raw_topic.physical_topic_name;

    assert_eq!(processor_name, "EAPItemsProcessor");

    let resource_id =
        get_cogs_label(processor_name).unwrap_or_else(|| format!("{}_processor", storage.name));
    let (cadence, idle_timeout) = make_flush_timers(consumer_config);

    EapPipeline::new(
        make_processor_stage(
            crate::processors::eap_items::process_message_row_binary,
            processor_config,
        ),
        processing_concurrency,
        make_dlq_handler(
            &shared.dlq_producer,
            consumer_config.dlq_topic.as_ref(),
            dry_run,
        ),
        make_batch_stage(
            consumer_config.max_batch_size as u64,
            consumer_config.max_batch_size_calculation,
        ),
        cadence,
        idle_timeout,
        make_writer_stage(&shared.ch_writer),
        clickhouse_concurrency,
        make_commit_log_stage(
            &shared.commit_log_producer,
            consumer_config.commit_log_topic.as_ref(),
            source_topic_name,
            consumer_group,
            dry_run,
        ),
        make_cogs_stage(
            &shared.cogs_producer,
            &consumer_config.accountant_topic,
            resource_id,
            dry_run,
            consumer_config.env.record_cogs,
        ),
    )
}

fn make_faf_pipeline(
    shared: &SharedResources,
    processor: crate::processors::ProcessingFunction,
    consumer_config: &config::ConsumerConfig,
    processor_config: &ProcessorConfig,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
) -> FireAndForgetPipeline {
    let (cadence, idle_timeout) = make_flush_timers(consumer_config);

    FireAndForgetPipeline::new(
        make_processor_stage(processor, processor_config),
        processing_concurrency,
        make_batch_stage(
            consumer_config.max_batch_size as u64,
            consumer_config.max_batch_size_calculation,
        ),
        cadence,
        idle_timeout,
        make_writer_stage(&shared.ch_writer),
        clickhouse_concurrency,
    )
}

// ── Rebalance loop ─────────────────────────────────────────

async fn run_with_rebalance<P: Pipeline<Output = BatchMetadata>>(
    source: &KafkaSource,
    build_pipeline: impl Fn() -> P,
) -> usize {
    loop {
        let pipeline = build_pipeline();
        let mut tracker = OffsetTracker::new(Duration::from_secs(1), source.committer());
        let result = pipeline.stream(source.stream()).commit(&mut tracker);

        match result.await {
            Ok(PipelineExit::Rebalance) => {
                tracing::info!("Rebalance detected, restarting pipeline...");
                continue;
            }
            Ok(PipelineExit::Shutdown | PipelineExit::Complete) => {
                tracing::info!("Pipeline shutdown");
                return 0;
            }
            Err(e) => {
                tracing::error!("Pipeline failed: {}", e);
                return 1;
            }
        }
    }
}

// ── Entry point ────────────────────────────────────────────

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn pull_consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
    max_poll_interval_ms: usize,
    dry_run_latency_ms: u64,
    use_row_binary: bool,
) -> usize {
    py.allow_threads(|| {
        pull_consumer_impl(
            consumer_group,
            auto_offset_reset,
            no_strict_offset_reset,
            consumer_config_raw,
            processing_concurrency,
            clickhouse_concurrency,
            max_poll_interval_ms,
            dry_run_latency_ms,
            use_row_binary,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn pull_consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
    max_poll_interval_ms: usize,
    dry_run_latency_ms: u64,
    use_row_binary: bool,
) -> usize {
    setup_logging();
    crate::init_sentry_options().expect("failed to initialize sentry-options");

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw)
        .expect("failed to parse consumer config");

    assert_eq!(
        consumer_config.storages.len(),
        1,
        "pull consumer only supports a single storage"
    );

    let storage = consumer_config.storages[0].clone();
    let processor_name = storage.message_processor.python_class_name.clone();
    let env_config = consumer_config.env.clone();

    // Sentry
    let mut _sentry_guard = None;
    if let Some(ref dsn) = consumer_config.env.sentry_dsn {
        std::env::set_var("RUST_BACKTRACE", "1");
        _sentry_guard = Some(setup_sentry(dsn));
    }

    // Metrics
    {
        let tags = [
            ("storage", storage.name.clone()),
            ("consumer_group", consumer_group.to_owned()),
        ];
        sentry::configure_scope(|scope| {
            scope.set_tag("storage", &storage.name);
            scope.set_tag("consumer_group", consumer_group);
        });
        if let Some(backend) = create_dogstatsd_backend(&env_config, "snuba.consumer", &tags) {
            metrics::init(backend).unwrap();
        }
    }

    let processor = match resolve_processor(&processor_name) {
        Ok(f) => f,
        Err(msg) => {
            tracing::error!("{msg}");
            return 1;
        }
    };

    // Validate pipeline type
    let is_eap = EAP_PROCESSORS.contains(&processor_name.as_str());
    let is_faf = FIRE_AND_FORGET_PROCESSORS.contains(&processor_name.as_str());
    if !is_eap && !is_faf {
        tracing::error!("{processor_name} is not supported by the pull consumer");
        return 1;
    }

    let dry_run = dry_run_latency_ms > 0;
    let dry_run_latency = if dry_run {
        Some(Duration::from_millis(dry_run_latency_ms))
    } else {
        None
    };

    tracing::info!(
        storage = storage.name,
        processor = processor_name.as_str(),
        pipeline = if is_eap { "eap" } else { "fire_and_forget" },
        dry_run,
        dry_run_latency_ms,
        use_row_binary,
        "Starting pull consumer",
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let exit_code = rt.block_on(async {
        let source = make_kafka_source(
            &consumer_config,
            consumer_group,
            auto_offset_reset,
            no_strict_offset_reset,
            max_poll_interval_ms,
        );
        let processor_config = make_processor_config(&storage, &env_config);

        let shared = make_shared_resources(
            &consumer_config,
            &storage,
            &processor_config,
            use_row_binary,
            dry_run,
            dry_run_latency,
        );

        let result = if is_eap {
            run_with_rebalance(&source, || {
                make_eap_pipeline(
                    &shared,
                    &consumer_config,
                    &storage,
                    &processor_config,
                    consumer_group,
                    processing_concurrency,
                    clickhouse_concurrency,
                    dry_run,
                )
            })
            .await
        } else {
            run_with_rebalance(&source, || {
                make_faf_pipeline(
                    &shared,
                    processor,
                    &consumer_config,
                    &processor_config,
                    processing_concurrency,
                    clickhouse_concurrency,
                )
            })
            .await
        };

        source.shutdown();
        result
    });

    exit_code
}
