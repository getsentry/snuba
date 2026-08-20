use sentry_arroyo::processing::stream::{MessageMetadata, PipelineEnvelope, StreamCollector};

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

    fn on_complete(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
