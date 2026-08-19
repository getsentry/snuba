use std::sync::{Arc, Mutex};

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{Producer, ProducerError};
use sentry_arroyo::types::TopicOrPartition;

/// Record of a single produce call.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProduceCall {
    pub destination: String,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
}

/// Shared handle to inspect produce calls after the producer
/// has been moved into a stage.
pub type ProduceCalls = Arc<Mutex<Vec<ProduceCall>>>;

/// Mock sync producer that records all produce calls for test assertions.
/// Create via `MockProducer::new()`, which returns `(MockProducer, ProduceCalls)`.
pub struct MockProducer {
    calls: ProduceCalls,
}

impl MockProducer {
    pub fn new() -> (Self, ProduceCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                calls: calls.clone(),
            },
            calls,
        )
    }
}

impl Producer<KafkaPayload> for MockProducer {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        self.calls.lock().unwrap().push(ProduceCall {
            destination: format!("{destination:?}"),
            key: payload.key().cloned(),
            payload: payload.payload().cloned(),
        });
        Ok(())
    }
}
