mod functions;
mod profiles;
mod querylog;
mod spans;

use crate::types::{BadMessage, BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;

type ProcessingFunction =
    fn(KafkaPayload, KafkaMessageMetadata) -> Result<BytesInsertBatch, BadMessage>;

pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
    match name {
        "FunctionsMessageProcessor" => Some(functions::process_message),
        "ProfilesMessageProcessor" => Some(profiles::process_message),
        "QuerylogProcessor" => Some(querylog::process_message),
        "SpansMessageProcessor" => Some(spans::process_message),
        _ => None,
    }
}
