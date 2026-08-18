use sentry_arroyo::processing::stream::{MessageMetadata, PipelineEnvelope, StreamCollector};

use crate::types::InsertBatch;

/// Test collector that captures emitted batches for assertions.
pub struct TestCollector {
    pub batches: Vec<Vec<InsertBatch>>,
}

impl TestCollector {
    pub fn new() -> Self {
        Self {
            batches: Vec::new(),
        }
    }
}

impl StreamCollector<Vec<InsertBatch>> for TestCollector {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<Vec<InsertBatch>>) {
        self.batches.push(envelope.payload.clone());
    }

    fn on_drop(&mut self, _metadata: &MessageMetadata) {}

    fn on_reject(&mut self, _metadata: &MessageMetadata) {}

    fn on_complete(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
