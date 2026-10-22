use sentry_arroyo::processing::stream::{
    BoxError, MessageMetadata, PipelineEnvelope, PipelineExit, StreamCollector,
};

use crate::pull::batch::batch_metadata::BatchMetadata;

/// Test collector that captures emitted batch metadata for assertions.
pub struct TestCollector {
    pub batches: Vec<BatchMetadata>,
}

impl TestCollector {
    pub fn new() -> Self {
        Self {
            batches: Vec::new(),
        }
    }
}

impl StreamCollector<BatchMetadata> for TestCollector {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<BatchMetadata>) {
        self.batches.push(envelope.payload.clone());
    }

    fn on_drop(&mut self, _metadata: &MessageMetadata) {}

    fn on_reject(&mut self, _metadata: &MessageMetadata) {}

    fn on_fail(&mut self, _error: &BoxError) -> Result<(), BoxError> {
        Ok(())
    }

    fn on_exit(&mut self, _reason: PipelineExit) -> Result<(), BoxError> {
        Ok(())
    }

    fn after_each(&mut self) -> Result<(), BoxError> {
        Ok(())
    }

    fn on_complete(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
