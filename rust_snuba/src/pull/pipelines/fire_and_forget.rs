use std::time::Duration;

use futures::Stream;
use sentry_arroyo::processing::stream::{
    BatchStage, LogHandler, PipelineExt, PullSource, StageResult,
};

use crate::pull::batch::batch_metadata::BatchMetadata;
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::processor_stage::ProcessorStage;

/// Pipeline for consumers with no side-effect stages.
///
/// Source → Processor → [log rejections] → Batch → Writer
///
/// Covers: outcomes, profiles, profile_chunks, functions,
/// llm_proxy_cost, groupedmessages, groupassignees, querylog,
/// replays, group_attributes.
pub struct FireAndForgetPipeline {
    source: Box<dyn PullSource>,
    processor: ProcessorStage,
    rejection_handler: LogHandler,
    batch: BatchStage<PipelineBatch, PipelineBatchBuffer>,
    max_batch_time: Option<Duration>,
    idle_timeout: Option<Duration>,
    writer: ClickHouseWriterStage,
    writer_concurrency: usize,
}

impl FireAndForgetPipeline {
    pub fn new(
        source: impl PullSource + 'static,
        processor: ProcessorStage,
        batch: BatchStage<PipelineBatch, PipelineBatchBuffer>,
        max_batch_time: Option<Duration>,
        idle_timeout: Option<Duration>,
        writer: ClickHouseWriterStage,
        writer_concurrency: usize,
    ) -> Self {
        Self {
            source: Box::new(source),
            processor,
            rejection_handler: LogHandler,
            batch,
            max_batch_time,
            idle_timeout,
            writer,
            writer_concurrency,
        }
    }

    pub fn stream(&self) -> impl Stream<Item = StageResult<BatchMetadata>> + '_ {
        self.source
            .stream()
            .apply(&self.processor)
            .apply_with_timer(&self.batch, self.idle_timeout, self.max_batch_time)
            .apply_concurrent(&self.writer, self.writer_concurrency)
            .on_reject(&self.rejection_handler)
    }
}
