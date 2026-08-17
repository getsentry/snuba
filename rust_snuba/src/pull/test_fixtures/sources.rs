use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

use futures::stream::Stream;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{
    MessageMetadata, OffsetCommitter, PipelineEnvelope, PullSource, StageResult,
};
use sentry_arroyo::types::{Partition, Topic};
use std::sync::Arc;

/// In-memory source for testing. Constructs envelopes from raw payloads
/// with sequential offsets on partition 0.
pub struct VecSource {
    messages: Mutex<Vec<StageResult<KafkaPayload>>>,
    committer: super::committers::MockCommitter,
}

impl VecSource {
    pub fn from_payloads(payloads: Vec<KafkaPayload>) -> Self {
        let messages = payloads
            .into_iter()
            .enumerate()
            .map(|(i, kp)| {
                let md = MessageMetadata {
                    partition: Partition::new(Topic::new("test"), 0),
                    offset: i as u64,
                    timestamp: chrono::Utc::now(),
                };
                StageResult::Emit(PipelineEnvelope::new(kp.clone(), md, Arc::new(kp)))
            })
            .collect();

        Self {
            messages: Mutex::new(messages),
            committer: super::committers::MockCommitter::new(),
        }
    }
}

impl PullSource for VecSource {
    fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>> {
        let messages: Vec<_> = self.messages.lock().unwrap().drain(..).collect();
        Box::pin(futures::stream::iter(messages))
    }

    fn committer(&self) -> &dyn OffsetCommitter {
        &self.committer
    }

    fn shutdown(&self) {}
}
