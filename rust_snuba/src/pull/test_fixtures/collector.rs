use sentry_arroyo::processing::stream::{MessageMetadata, PipelineEnvelope, StreamCollector};

use crate::pull::batch::pipeline_batch::PipelineBatch;

/// Test collector that captures emitted batches for assertions.
pub struct TestCollector {
    pub batches: Vec<PipelineBatch>,
}

impl TestCollector {
    pub fn new() -> Self {
        Self {
            batches: Vec::new(),
        }
    }
}

impl StreamCollector<PipelineBatch> for TestCollector {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<PipelineBatch>) {
        self.batches.push(envelope.payload.clone());
    }

    fn on_drop(&mut self, _metadata: &MessageMetadata) {}

    fn on_reject(&mut self, _metadata: &MessageMetadata) {}

    fn on_complete(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
