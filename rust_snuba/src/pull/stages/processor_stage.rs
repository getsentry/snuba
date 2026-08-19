use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, RejectionReason, Stage, StageResult};

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
        let processor = self.processor;
        let config = self.config.clone();
        let partition = envelope.metadata.partition.index;
        let offset = envelope.metadata.offset;
        let timestamp = envelope.metadata.timestamp;
        let payload = envelope.payload;

        let result = tokio::task::spawn_blocking(move || {
            let metadata = KafkaMessageMetadata {
                partition,
                offset,
                timestamp,
            };
            processor(payload, metadata, &config)
        })
        .await;

        match result {
            Ok(Ok(batch)) if batch.rows.num_rows == 0 => StageResult::Drop {
                metadata: envelope.metadata,
            },
            Ok(Ok(batch)) => {
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
            Ok(Err(e)) => {
                tracing::warn!(
                    "Processor error at {:?}:{}, rejecting: {}",
                    envelope.metadata.partition,
                    envelope.metadata.offset,
                    e
                );
                StageResult::Reject {
                    metadata: envelope.metadata,
                    raw: envelope.raw,
                    reason: RejectionReason::Invalid,
                }
            }
            Err(join_err) => StageResult::Fail(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("processor task panicked: {}", join_err),
            ))),
        }
    }

    fn name(&self) -> &str {
        "processor"
    }
}
