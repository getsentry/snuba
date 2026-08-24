use std::time::Duration;

use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::metrics;
use sentry_arroyo::processing::stream::{
    BatchStage, DlqHandler, KafkaSource, OffsetTracker, Pipeline, PipelineExit, PipelineExt,
    PullSource,
};
use sentry_arroyo::types::{Topic, TopicOrPartition};

use crate::config::{self, ProcessorConfig};
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::statsd::create_dogstatsd_backend;
use crate::processors::{get_cogs_label, get_processing_function, ProcessingFunctionType};
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::pipelines::eap::EapPipeline;
use crate::pull::pipelines::fire_and_forget::FireAndForgetPipeline;
use crate::pull::producers::DryRunProducer;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::cogs_stage::CogsStage;
use crate::pull::stages::commit_log_stage::CommitLogStage;
use crate::pull::stages::processor_stage::ProcessorStage;
use crate::pull::writer::DryRunWriter;
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
const EAP_PROCESSORS: &[&str] = &["EAPItemsProcessor", "GenericCountersMetricsProcessor"];

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

    // Resolve processor
    let processor = match get_processing_function(&processor_name) {
        Some(ProcessingFunctionType::ProcessingFunction(f)) => f,
        Some(ProcessingFunctionType::ProcessingFunctionWithReplacements(_)) => {
            tracing::error!("{processor_name} is a replacement processor — not supported");
            return 1;
        }
        None => {
            tracing::error!("Unknown processor: {processor_name}");
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

    tracing::info!(
        storage = storage.name,
        processor = processor_name.as_str(),
        pipeline = if is_eap { "eap" } else { "fire_and_forget" },
        dry_run,
        dry_run_latency_ms,
        "Starting pull consumer",
    );

    // Kafka source
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

    let processor_config = ProcessorConfig {
        env_config: env_config.clone(),
        storage_name: storage.name.clone(),
        ..Default::default()
    };

    let max_batch_size = consumer_config.max_batch_size as u64;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    // Build tokio runtime and run
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let exit_code = rt.block_on(async {
        // KafkaSource must be created inside the tokio runtime
        // (rdkafka's StreamConsumer requires a reactor)
        let source = KafkaSource::new(kafka_config, &[topic]);

        let result = if is_eap {
            run_eap(
                &source,
                processor,
                &processor_config,
                &storage,
                &consumer_config,
                &env_config,
                consumer_group,
                processing_concurrency,
                max_batch_size,
                max_batch_time,
                clickhouse_concurrency,
                dry_run_latency_ms,
            )
            .await
        } else {
            run_fire_and_forget(
                &source,
                processor,
                &processor_config,
                &storage,
                processing_concurrency,
                max_batch_size,
                max_batch_time,
                clickhouse_concurrency,
                dry_run_latency_ms,
            )
            .await
        };

        source.shutdown();
        result
    });

    exit_code
}

#[allow(clippy::too_many_arguments)]
async fn run_fire_and_forget(
    source: &KafkaSource,
    processor: crate::processors::ProcessingFunction,
    processor_config: &ProcessorConfig,
    storage: &config::StorageConfig,
    processing_concurrency: usize,
    max_batch_size: u64,
    max_batch_time: Duration,
    clickhouse_concurrency: usize,
    dry_run_latency_ms: u64,
) -> usize {
    let dry_run = dry_run_latency_ms > 0;
    loop {
        let writer = if dry_run {
            ClickHouseWriterStage::new(DryRunWriter::new(Duration::from_millis(dry_run_latency_ms)))
        } else {
            ClickHouseWriterStage::new(ClickhouseClient::new(
                &storage.clickhouse_cluster,
                &storage.clickhouse_table_name,
                storage.name.clone(),
                InsertFormat::JsonEachRow,
                None,
            ))
        };

        let pipeline = FireAndForgetPipeline::new(
            ProcessorStage::new(processor, processor_config.clone()),
            processing_concurrency,
            BatchStage::new(PipelineBatchBuffer::new(), max_batch_size, u64::MAX),
            Some(max_batch_time),
            None,
            writer,
            clickhouse_concurrency,
        );

        let mut tracker = OffsetTracker::new(Duration::from_secs(1), source.committer());
        match pipeline.stream(source.stream()).commit(&mut tracker).await {
            Ok(PipelineExit::Rebalance) => {
                tracing::info!("Rebalance detected, restarting pipeline");
                continue;
            }
            Ok(PipelineExit::Shutdown | PipelineExit::Complete) => return 0,
            Err(e) => {
                tracing::error!("Pipeline failed: {}", e);
                return 1;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_eap(
    source: &KafkaSource,
    processor: crate::processors::ProcessingFunction,
    processor_config: &ProcessorConfig,
    storage: &config::StorageConfig,
    consumer_config: &config::ConsumerConfig,
    env_config: &config::EnvConfig,
    consumer_group: &str,
    processing_concurrency: usize,
    max_batch_size: u64,
    max_batch_time: Duration,
    clickhouse_concurrency: usize,
    dry_run_latency_ms: u64,
) -> usize {
    let dry_run = dry_run_latency_ms > 0;
    let processor_name = &storage.message_processor.python_class_name;
    let source_topic_name = &consumer_config.raw_topic.physical_topic_name;

    loop {
        let writer = if dry_run {
            ClickHouseWriterStage::new(DryRunWriter::new(Duration::from_millis(dry_run_latency_ms)))
        } else {
            ClickHouseWriterStage::new(ClickhouseClient::new(
                &storage.clickhouse_cluster,
                &storage.clickhouse_table_name,
                storage.name.clone(),
                InsertFormat::RowBinary,
                None,
            ))
        };

        let dlq_handler = if dry_run {
            DlqHandler::new(
                DryRunProducer,
                TopicOrPartition::Topic(Topic::new("dry-run-dlq")),
            )
        } else if let Some(ref topic_config) = consumer_config.dlq_topic {
            let producer = KafkaProducer::new(KafkaConfig::new_producer_config(
                vec![],
                Some(topic_config.broker_config.clone()),
            ));
            DlqHandler::new(
                producer,
                TopicOrPartition::Topic(Topic::new(&topic_config.physical_topic_name)),
            )
        } else {
            DlqHandler::new(
                DryRunProducer,
                TopicOrPartition::Topic(Topic::new("no-dlq-configured")),
            )
        };

        let commit_log = if dry_run {
            CommitLogStage::new(
                DryRunProducer,
                Topic::new("dry-run-commit-log"),
                Topic::new(source_topic_name),
                consumer_group.to_string(),
            )
        } else if let Some(ref topic_config) = consumer_config.commit_log_topic {
            let producer = KafkaProducer::new(KafkaConfig::new_producer_config(
                vec![],
                Some(topic_config.broker_config.clone()),
            ));
            CommitLogStage::new(
                producer,
                Topic::new(&topic_config.physical_topic_name),
                Topic::new(source_topic_name),
                consumer_group.to_string(),
            )
        } else {
            CommitLogStage::new(
                DryRunProducer,
                Topic::new("no-commit-log-configured"),
                Topic::new(source_topic_name),
                consumer_group.to_string(),
            )
        };

        let resource_id =
            get_cogs_label(processor_name).unwrap_or_else(|| format!("{}_processor", storage.name));

        let cogs = if dry_run || !env_config.record_cogs {
            CogsStage::new(DryRunProducer, Topic::new("dry-run-cogs"), resource_id)
        } else {
            let producer = KafkaProducer::new(KafkaConfig::new_producer_config(
                vec![],
                Some(consumer_config.accountant_topic.broker_config.clone()),
            ));
            CogsStage::new(
                producer,
                Topic::new(&consumer_config.accountant_topic.physical_topic_name),
                resource_id,
            )
        };

        let pipeline = EapPipeline::new(
            ProcessorStage::new(processor, processor_config.clone()),
            processing_concurrency,
            dlq_handler,
            BatchStage::new(PipelineBatchBuffer::new(), max_batch_size, u64::MAX),
            Some(max_batch_time),
            None,
            writer,
            clickhouse_concurrency,
            commit_log,
            cogs,
        );

        let mut tracker = OffsetTracker::new(Duration::from_secs(1), source.committer());
        match pipeline.stream(source.stream()).commit(&mut tracker).await {
            Ok(PipelineExit::Rebalance) => {
                tracing::info!("Rebalance detected, restarting pipeline");
                continue;
            }
            Ok(PipelineExit::Shutdown | PipelineExit::Complete) => return 0,
            Err(e) => {
                tracing::error!("Pipeline failed: {}", e);
                return 1;
            }
        }
    }
}
