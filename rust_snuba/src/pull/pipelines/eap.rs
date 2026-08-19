use std::time::Duration;

use futures::Stream;
use sentry_arroyo::processing::stream::{
    BatchStage, DlqHandler, PipelineExt, PullSource, StageResult,
};

use crate::pull::batch::batch_metadata::BatchMetadata;
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::cogs_stage::CogsStage;
use crate::pull::stages::commit_log_stage::CommitLogStage;
use crate::pull::stages::processor_stage::ProcessorStage;

/// Pipeline for EAP items — includes DLQ, commit log, and COGS stages.
///
/// Source → Processor → [DLQ rejections] → Batch → Writer → CommitLog → COGS
///
/// Covers: eap_items, generic_metrics (same shape).
pub struct EapPipeline {
    source: Box<dyn PullSource>,
    processor: ProcessorStage,
    dlq_handler: DlqHandler,
    batch: BatchStage<PipelineBatch, PipelineBatchBuffer>,
    max_batch_time: Option<Duration>,
    idle_timeout: Option<Duration>,
    writer: ClickHouseWriterStage,
    writer_concurrency: usize,
    commit_log: CommitLogStage,
    cogs: CogsStage,
}

impl EapPipeline {
    pub fn new(
        source: impl PullSource + 'static,
        processor: ProcessorStage,
        dlq_handler: DlqHandler,
        batch: BatchStage<PipelineBatch, PipelineBatchBuffer>,
        max_batch_time: Option<Duration>,
        idle_timeout: Option<Duration>,
        writer: ClickHouseWriterStage,
        writer_concurrency: usize,
        commit_log: CommitLogStage,
        cogs: CogsStage,
    ) -> Self {
        Self {
            source: Box::new(source),
            processor,
            dlq_handler,
            batch,
            max_batch_time,
            idle_timeout,
            writer,
            writer_concurrency,
            commit_log,
            cogs,
        }
    }

    pub fn stream(&self) -> impl Stream<Item = StageResult<BatchMetadata>> + '_ {
        self.source
            .stream()
            .apply(&self.processor)
            .apply_with_timer(&self.batch, self.idle_timeout, self.max_batch_time)
            .apply_concurrent(&self.writer, self.writer_concurrency)
            .apply(&self.commit_log)
            .apply(&self.cogs)
            .on_reject(&self.dlq_handler)
    }
}
