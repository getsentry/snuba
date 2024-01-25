use crate::config::ProcessorConfig;

use rust_arroyo::backends::kafka::types::KafkaPayload;

use crate::types::{InsertBatch, InsertOrReplacement, KafkaMessageMetadata};

#[allow(dead_code)]
pub fn process_message_with_replacement(
    _payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertOrReplacement<InsertBatch>> {
    unimplemented!();
}
