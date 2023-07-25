#![allow(dead_code)]

mod querylog;
use crate::types::BytesInsertBatch;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::InvalidMessage;

pub fn get_processing_function(
    name: &str,
) -> Option<fn(KafkaPayload) -> Result<BytesInsertBatch, InvalidMessage>> {
    match name {
        "QuerylogProcessor" => Some(querylog::process_message),
        _ => None,
    }
}
