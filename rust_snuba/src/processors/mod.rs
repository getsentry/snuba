mod functions;
mod metrics_summaries;
mod profiles;
mod querylog;
mod replays;
mod spans;
mod utils;

use crate::types::{InsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;

pub type ProcessingFunction = fn(KafkaPayload, KafkaMessageMetadata) -> anyhow::Result<InsertBatch>;

pub static PROCESSING_FUNCTIONS: phf::Map<&'static str, ProcessingFunction> = phf::phf_map! {
    "FunctionsMessageProcessor" => functions::process_message,
    "ProfilesMessageProcessor" => profiles::process_message,
    "QuerylogProcessor" => querylog::process_message,
    "ReplaysProcessor" => replays::process_message,
    "SpansMessageProcessor" => spans::process_message,
    "MetricsSummariesMessageProcessor" => metrics_summaries::process_message,
};
