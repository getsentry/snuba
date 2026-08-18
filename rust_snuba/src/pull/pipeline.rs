use sentry_arroyo::processing::stream::{BatchStage, PipelineExt, PullSource, StageResult};

use crate::config::ProcessorConfig;
use crate::processors::ProcessingFunction;
use crate::types::InsertBatch;

use super::batch::buffer::InsertBatchBuffer;
use super::stages::noop_stage::NoopStage;
use super::stages::processor_stage::ProcessorStage;

/// Configuration for a pull-based snuba consumer pipeline.
pub struct PipelineConfig {
    pub processor: ProcessingFunction,
    pub processor_config: ProcessorConfig,
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
}

/// A fully wired snuba consumer pipeline.
///
/// Owns the source and all stages. Call `stream()` to get the
/// pipeline as an async stream, then `.commit()` or append
/// additional stages before committing.
///
/// Pipeline shape:
///   Source → Processor → Batch → Writer → CommitLog → COGS
pub struct Pipeline<S: PullSource> {
    source: S,
    processor: ProcessorStage,
    batch: BatchStage<InsertBatch, InsertBatchBuffer>,
    writer: NoopStage<Vec<InsertBatch>>,
    commit_log: NoopStage<Vec<InsertBatch>>,
    cogs: NoopStage<Vec<InsertBatch>>,
}

impl<S: PullSource> Pipeline<S> {
    pub fn build(source: S, config: &PipelineConfig) -> Self {
        Self {
            source,
            processor: ProcessorStage::new(config.processor, config.processor_config.clone()),
            batch: BatchStage::new(
                InsertBatchBuffer::new(),
                config.max_batch_rows,
                config.max_batch_bytes,
            ),
            writer: NoopStage::new("clickhouse_writer"),
            commit_log: NoopStage::new("commit_log"),
            cogs: NoopStage::new("cogs"),
        }
    }

    pub fn stream(
        &self,
    ) -> impl futures::stream::Stream<Item = StageResult<Vec<InsertBatch>>> + '_ {
        self.source
            .stream()
            .apply(&self.processor)
            .apply(&self.batch)
            .apply(&self.writer)
            .apply(&self.commit_log)
            .apply(&self.cogs)
    }
}
