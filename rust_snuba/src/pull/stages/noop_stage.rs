use std::marker::PhantomData;

use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

/// A pass-through stage that emits every envelope unchanged.
/// Used as a stub for stages not yet implemented (writer, commit_log, cogs).
pub struct NoopStage<T> {
    name: &'static str,
    _marker: PhantomData<T>,
}

impl<T> NoopStage<T> {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static> Stage for NoopStage<T> {
    type In = T;
    type Out = T;

    async fn process(&self, envelope: PipelineEnvelope<T>) -> StageResult<T> {
        StageResult::Emit(envelope)
    }

    fn name(&self) -> &str {
        self.name
    }
}
