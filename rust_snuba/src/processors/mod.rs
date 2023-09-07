mod querylog;
use crate::types::{BytesInsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::InvalidMessage;

type ProcessingFunction =
    fn(KafkaPayload, KafkaMessageMetadata) -> Result<BytesInsertBatch, InvalidMessage>;

pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
    match name {
        "QuerylogProcessor" => Some(querylog::process_message),
        _ => None,
    }
}
