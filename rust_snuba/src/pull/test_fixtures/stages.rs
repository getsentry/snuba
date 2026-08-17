use std::sync::{Arc, Mutex};

use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

use crate::types::InsertBatch;

/// Collecting stage — captures emitted items for test assertions.
pub struct CollectStage {
    pub collected: Arc<Mutex<Vec<InsertBatch>>>,
}

impl CollectStage {
    pub fn new() -> (Self, Arc<Mutex<Vec<InsertBatch>>>) {
        let collected = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                collected: collected.clone(),
            },
            collected,
        )
    }
}

impl Stage for CollectStage {
    type In = InsertBatch;
    type Out = InsertBatch;

    async fn process(&self, envelope: PipelineEnvelope<InsertBatch>) -> StageResult<InsertBatch> {
        self.collected
            .lock()
            .unwrap()
            .push(envelope.payload.clone());
        StageResult::Emit(envelope)
    }

    fn name(&self) -> &str {
        "collect"
    }
}
