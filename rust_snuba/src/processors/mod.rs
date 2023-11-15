mod functions;
mod profiles;
mod querylog;
mod spans;
mod utils;

use crate::types::{BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use std::sync::Arc;

type ProcessingFunction =
    fn(Arc<KafkaPayload>, KafkaMessageMetadata) -> anyhow::Result<BytesInsertBatch>;

pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
    match name {
        "FunctionsMessageProcessor" => Some(functions::process_message),
        "ProfilesMessageProcessor" => Some(profiles::process_message),
        "QuerylogProcessor" => Some(querylog::process_message),
        "SpansMessageProcessor" => Some(spans::process_message),
        _ => None,
    }
}
