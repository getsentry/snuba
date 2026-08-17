use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

use crate::config::ProcessorConfig;
use crate::processors::ProcessingFunction;
use crate::types::{InsertBatch, KafkaMessageMetadata};

/// Wraps a snuba ProcessingFunction as a pull-based Stage.
///
/// Takes a KafkaPayload, extracts metadata from the envelope,
/// calls the processor function, and emits the InsertBatch.
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
    type Out = InsertBatch;

    async fn process(&self, envelope: PipelineEnvelope<KafkaPayload>) -> StageResult<InsertBatch> {
        let metadata = KafkaMessageMetadata {
            partition: envelope.metadata.partition.index,
            offset: envelope.metadata.offset,
            timestamp: envelope.metadata.timestamp,
        };

        match (self.processor)(envelope.payload, metadata, &self.config) {
            Ok(batch) if batch.rows.num_rows == 0 => {
                // Processor returned empty rows (e.g., filtered message).
                // Track the offset but don't emit downstream.
                StageResult::Drop {
                    metadata: envelope.metadata,
                }
            }
            Ok(batch) => StageResult::Emit(PipelineEnvelope::new(
                batch,
                envelope.metadata,
                envelope.raw,
            )),
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
