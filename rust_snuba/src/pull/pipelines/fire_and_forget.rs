use std::time::Duration;

use futures::Stream;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{
    BatchStage, LogHandler, Pipeline, PipelineExt, StageResult,
};

use crate::pull::batch::batch_metadata::BatchMetadata;
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::processor_stage::ProcessorStage;

/// Pipeline for consumers with no side-effect stages.
///
/// Processor → [log rejections] → Batch → Writer
///
/// Covers: outcomes, profiles, profile_chunks, functions,
/// llm_proxy_cost, groupedmessages, groupassignees, querylog,
/// replays, group_attributes.
pub struct FireAndForgetPipeline {
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
        processor: ProcessorStage,
        batch: BatchStage<PipelineBatch, PipelineBatchBuffer>,
        max_batch_time: Option<Duration>,
        idle_timeout: Option<Duration>,
        writer: ClickHouseWriterStage,
        writer_concurrency: usize,
    ) -> Self {
        Self {
            processor,
            rejection_handler: LogHandler,
            batch,
            max_batch_time,
            idle_timeout,
            writer,
            writer_concurrency,
        }
    }
}

impl Pipeline for FireAndForgetPipeline {
    type Output = BatchMetadata;

    fn stream<'a>(
        &'a self,
        source: impl Stream<Item = StageResult<KafkaPayload>> + 'a,
    ) -> impl Stream<Item = StageResult<BatchMetadata>> + 'a {
        source
            .apply(&self.processor)
            .apply_with_timer(&self.batch, self.idle_timeout, self.max_batch_time)
            .apply_concurrent(&self.writer, self.writer_concurrency)
            .on_reject(&self.rejection_handler)
    }
}
