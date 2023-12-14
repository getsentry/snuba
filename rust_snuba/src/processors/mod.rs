mod functions;
mod metrics_summaries;
mod profiles;
mod querylog;
mod spans;
mod utils;

use crate::types::{InsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;

pub type ProcessingFunction = fn(KafkaPayload, KafkaMessageMetadata) -> anyhow::Result<InsertBatch>;

pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
    match name {
        "FunctionsMessageProcessor" => Some(functions::process_message),
        "ProfilesMessageProcessor" => Some(profiles::process_message),
        "QuerylogProcessor" => Some(querylog::process_message),
        "SpansMessageProcessor" => Some(spans::process_message),
        "MetricsSummariesMessageProcessor" => Some(metrics_summaries::process_message),
        _ => None,
    }
}
