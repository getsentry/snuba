use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{Producer, ProducerError};
use sentry_arroyo::types::TopicOrPartition;

/// A Kafka producer that logs what it would produce and drops the message.
/// Zero state, zero allocations, zero memory growth.
///
/// Used in staging/dry-run deployments to exercise the full pipeline
/// (including commit log and COGS serialization) without actually
/// producing to Kafka.
pub struct DryRunProducer;

impl Producer<KafkaPayload> for DryRunProducer {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        tracing::info!(
            destination = ?destination,
            key_bytes = payload.key().map(|k| k.len()),
            payload_bytes = payload.payload().map(|p| p.len()),
            "dry-run: would have produced message"
        );
        Ok(())
    }
}
