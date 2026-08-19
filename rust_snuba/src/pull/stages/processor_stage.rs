use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

use crate::config::ProcessorConfig;
use crate::processors::ProcessingFunction;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::types::KafkaMessageMetadata;

/// Wraps a snuba ProcessingFunction as a pull-based Stage.
///
/// Takes a KafkaPayload, calls the processor function, and wraps
/// the result in a PipelineBatch with commit log offsets from
/// the envelope's partition/offset.
pub struct ProcessorStage {
    processor: ProcessingFunction,
    config: ProcessorConfig,
}

impl ProcessorStage {
    pub fn new(processor: ProcessingFunction, config: ProcessorConfig) -> Self {
        Self { processor, config }
    }
}

impl Stage for ProcessorStage {
    type In = KafkaPayload;
    type Out = PipelineBatch;

    async fn process(
        &self,
        envelope: PipelineEnvelope<KafkaPayload>,
    ) -> StageResult<PipelineBatch> {
        let metadata = KafkaMessageMetadata {
            partition: envelope.metadata.partition.index,
            offset: envelope.metadata.offset,
            timestamp: envelope.metadata.timestamp,
        };

        match (self.processor)(envelope.payload, metadata, &self.config) {
            Ok(batch) if batch.rows.num_rows == 0 => StageResult::Drop {
                metadata: envelope.metadata,
            },
            Ok(batch) => {
                let pipeline_batch = PipelineBatch::from_insert_batch(
                    batch,
                    envelope.metadata.partition.index,
                    envelope.metadata.offset,
                    envelope.metadata.timestamp,
                );
                StageResult::Emit(PipelineEnvelope::new(
                    pipeline_batch,
                    envelope.metadata,
                    envelope.raw,
                ))
            }
            Err(e) => StageResult::Fail(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
        }
    }

    fn name(&self) -> &str {
        "processor"
    }
}
