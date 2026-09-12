use crate::config;
use crate::config::ProcessorConfig;
use crate::processors::get_cogs_label;
use crate::pull::pipelines::eap::EapPipeline;
use crate::pull::pipelines::fire_and_forget::FireAndForgetPipeline;

use super::factories::{self, SharedResources};

#[allow(clippy::too_many_arguments)]
pub(super) fn make_eap_pipeline(
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
    let (cadence, idle_timeout) = factories::make_flush_timers(consumer_config);

    EapPipeline::new(
        factories::make_processor_stage(
            crate::processors::eap_items::process_message_row_binary,
            processor_config,
        ),
        processing_concurrency,
        factories::make_dlq_handler(
            &shared.dlq_producer,
            consumer_config.dlq_topic.as_ref(),
            dry_run,
        ),
        factories::make_batch_stage(
            consumer_config.max_batch_size as u64,
            consumer_config.max_batch_size_calculation,
        ),
        cadence,
        idle_timeout,
        factories::make_writer_stage(&shared.ch_writer),
        clickhouse_concurrency,
        factories::make_commit_log_stage(
            &shared.commit_log_producer,
            consumer_config.commit_log_topic.as_ref(),
            source_topic_name,
            consumer_group,
            dry_run,
        ),
        factories::make_cogs_stage(
            &shared.cogs_producer,
            &consumer_config.accountant_topic,
            resource_id,
            dry_run,
            consumer_config.env.record_cogs,
        ),
    )
}

pub(super) fn make_faf_pipeline(
    shared: &SharedResources,
    processor: crate::processors::ProcessingFunction,
    consumer_config: &config::ConsumerConfig,
    processor_config: &ProcessorConfig,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
) -> FireAndForgetPipeline {
    let (cadence, idle_timeout) = factories::make_flush_timers(consumer_config);

    FireAndForgetPipeline::new(
        factories::make_processor_stage(processor, processor_config),
        processing_concurrency,
        factories::make_batch_stage(
            consumer_config.max_batch_size as u64,
            consumer_config.max_batch_size_calculation,
        ),
        cadence,
        idle_timeout,
        factories::make_writer_stage(&shared.ch_writer),
        clickhouse_concurrency,
    )
}
