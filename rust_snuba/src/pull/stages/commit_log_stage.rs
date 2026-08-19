use std::sync::Arc;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::types::{Topic, TopicOrPartition};
use serde::Serialize;

use crate::pull::batch::batch_metadata::BatchMetadata;

#[derive(Serialize)]
struct Payload {
    offset: u64,
    orig_message_ts: f64,
    received_p99: Option<f64>,
}

/// Produces to the commit log topic after a successful ClickHouse write.
///
/// One Kafka message per partition in the batch's `CommitLogOffsets`.
/// Key: `{source_topic}:{partition}:{consumer_group}`.
/// Payload: JSON `{offset, orig_message_ts, received_p99}`.
///
/// Failure returns `Fail` — blocks the pipeline.
pub struct CommitLogStage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    destination: TopicOrPartition,
    source_topic: Topic,
    consumer_group: String,
}

impl CommitLogStage {
    pub fn new(
        producer: impl Producer<KafkaPayload> + 'static,
        destination: Topic,
        source_topic: Topic,
        consumer_group: String,
    ) -> Self {
        Self {
            producer: Arc::new(producer),
            destination: TopicOrPartition::Topic(destination),
            source_topic,
            consumer_group,
        }
    }
}

impl Stage for CommitLogStage {
    type In = BatchMetadata;
    type Out = BatchMetadata;

    async fn process(
        &self,
        envelope: PipelineEnvelope<BatchMetadata>,
    ) -> StageResult<BatchMetadata> {
        for (partition, entry) in &envelope.payload.commit_log_offsets.0 {
            let mut received_p99_values = entry.received_p99.clone();
            received_p99_values.sort();
            // NOTE: This p99 calculation is copied from the push model
            // (strategies/commit_log.rs:117). The formula `(len * 0.99)`
            // arguably computes P100 for small arrays, but we match the
            // existing behavior for consistency.
            let received_p99 = received_p99_values
                .get((received_p99_values.len() as f64 * 0.99) as usize)
                .map(|t| t.timestamp_millis() as f64 / 1000.0);

            let key = format!(
                "{}:{}:{}",
                self.source_topic.as_str(),
                partition,
                self.consumer_group
            );

            let payload = Payload {
                offset: entry.offset,
                orig_message_ts: entry.orig_message_ts.timestamp_millis() as f64 / 1000.0,
                received_p99,
            };

            let json = match serde_json::to_vec(&payload) {
                Ok(j) => j,
                Err(e) => return StageResult::Fail(Box::new(e)),
            };

            let kafka_payload = KafkaPayload::new(Some(key.into_bytes()), None, Some(json));

            if let Err(e) = self.producer.produce(&self.destination, kafka_payload) {
                return StageResult::Fail(Box::new(e));
            }
        }

        StageResult::Emit(envelope)
    }

    fn name(&self) -> &str {
        "commit_log"
    }
}
