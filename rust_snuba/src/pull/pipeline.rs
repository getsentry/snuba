use std::time::Duration;

use futures::Stream;
use sentry_arroyo::processing::stream::{BatchStage, PipelineExt, PullSource, StageResult};

use super::batch::batch_metadata::BatchMetadata;
use super::batch::buffer::PipelineBatchBuffer;
use super::batch::pipeline_batch::PipelineBatch;
use super::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use super::stages::noop_stage::NoopStage;
use super::stages::processor_stage::ProcessorStage;

/// A fully wired snuba consumer pipeline.
///
/// Takes pre-constructed stages — Pipeline is pure composition,
/// not a factory. The caller (test setup, main, factory function)
/// is responsible for constructing the concrete stages.
///
/// Pipeline shape:
///   Source → Processor → Batch → Writer → CommitLog → COGS
///                     PipelineBatch  ↓    BatchMetadata
///                               (rows consumed)
pub struct Pipeline {
    source: Box<dyn PullSource>,
    processor: ProcessorStage,
    batch: BatchStage<PipelineBatch, PipelineBatchBuffer>,
    max_batch_time: Option<Duration>,
    idle_timeout: Option<Duration>,
    writer: ClickHouseWriterStage,
    writer_concurrency: usize,
    commit_log: NoopStage<BatchMetadata>,
    cogs: NoopStage<BatchMetadata>,
}

impl Pipeline {
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
            batch,
            max_batch_time,
            idle_timeout,
            writer,
            writer_concurrency,
            commit_log: NoopStage::new("commit_log"),
            cogs: NoopStage::new("cogs"),
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
    }
}
