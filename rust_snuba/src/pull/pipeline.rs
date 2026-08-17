use std::time::Duration;

use sentry_arroyo::processing::stream::{
    BatchStage, OffsetTracker, PipelineExit, PipelineExt, PullSource, Stage, StageResult,
};

use crate::config::ProcessorConfig;
use crate::processors::ProcessingFunction;
use crate::types::InsertBatch;

use super::batch::buffer::InsertBatchBuffer;
use super::stages::noop_stage::NoopStage;
use super::stages::processor_stage::ProcessorStage;

/// Configuration for a pull-based snuba consumer pipeline.
pub struct PullPipelineConfig {
    pub processor: ProcessingFunction,
    pub processor_config: ProcessorConfig,
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
}

/// Run the snuba consumer pipeline.
///
/// Pipeline shape:
///   Source → Processor → Batch → Writer → CommitLog → COGS → commit
///
/// Writer, CommitLog, and COGS are currently no-ops.
/// The `observer` stage is inserted before commit — use a collecting
/// stage for test assertions, or a NoopStage in production.
pub async fn run_pipeline<S, O>(
    source: &S,
    config: &PullPipelineConfig,
    observer: &O,
) -> Result<PipelineExit, Box<dyn std::error::Error + Send>>
where
    S: PullSource,
    O: Stage<In = Vec<InsertBatch>, Out = Vec<InsertBatch>>,
{
    let processor = ProcessorStage::new(config.processor, config.processor_config.clone());
    let batch = BatchStage::new(
        InsertBatchBuffer::new(),
        config.max_batch_rows,
        config.max_batch_bytes,
    );
    let writer: NoopStage<Vec<InsertBatch>> = NoopStage::new("clickhouse_writer");
    let commit_log: NoopStage<Vec<InsertBatch>> = NoopStage::new("commit_log");
    let cogs: NoopStage<Vec<InsertBatch>> = NoopStage::new("cogs");

    let mut tracker = OffsetTracker::new(Duration::from_secs(5), source.committer());

    source
        .stream()
        .apply(&processor)
        .apply(&batch)
        .apply(&writer)
        .apply(&commit_log)
        .apply(&cogs)
        .apply(observer)
        .commit(&mut tracker)
        .await
}
