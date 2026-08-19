use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::types::{Topic, TopicOrPartition};
use serde::Serialize;

use crate::pull::batch::batch_metadata::BatchMetadata;

#[derive(Serialize)]
struct CogsMessage {
    timestamp: i64,
    shared_resource_id: String,
    app_feature: String,
    usage_unit: String,
    amount: u64,
}

/// Records COGS (cost of goods sold) usage by producing to the
/// shared-resources-usage topic.
///
/// Fire-and-forget: logs a warning on first failure, never blocks
/// the pipeline (returns `Emit` regardless of produce result).
///
/// NOTE: This produces on every batch flush (~every 2 seconds).
/// The push model aggregates for 60 seconds before producing.
/// Add an aggregation stage upstream if topic throughput becomes
/// a concern.
pub struct CogsStage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    destination: TopicOrPartition,
    resource_id: String,
    logged_warning: AtomicBool,
}

impl CogsStage {
    pub fn new(
        producer: impl Producer<KafkaPayload> + 'static,
        destination: Topic,
        resource_id: String,
    ) -> Self {
        Self {
            producer: Arc::new(producer),
            destination: TopicOrPartition::Topic(destination),
            resource_id,
            logged_warning: AtomicBool::new(false),
        }
    }
}

impl Stage for CogsStage {
    type In = BatchMetadata;
    type Out = BatchMetadata;

    async fn process(
        &self,
        envelope: PipelineEnvelope<BatchMetadata>,
    ) -> StageResult<BatchMetadata> {
        let timestamp = chrono::Utc::now().timestamp();

        for (app_feature, amount) in &envelope.payload.cogs_data.data {
            let message = CogsMessage {
                timestamp,
                shared_resource_id: self.resource_id.clone(),
                app_feature: app_feature.clone(),
                usage_unit: "bytes".to_string(),
                amount: *amount,
            };

            let payload = match serde_json::to_vec(&message) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("failed to serialize COGS message: {}", e);
                    continue;
                }
            };

            let kafka_payload = KafkaPayload::new(None, None, Some(payload));

            if let Err(err) = self.producer.produce(&self.destination, kafka_payload) {
                if !self.logged_warning.swap(true, Ordering::Relaxed) {
                    tracing::warn!(?err, "error producing COGS message");
                }
            }
        }

        StageResult::Emit(envelope)
    }

    fn name(&self) -> &str {
        "cogs"
    }
}
