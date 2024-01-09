mod functions;
mod metrics_summaries;
mod outcomes;
mod profiles;
mod querylog;
mod replays;
mod spans;
mod utils;
mod generic_metrics;

use crate::types::{InsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;

/// Interface for a processing function. If the function receives a message that it needs to skip
/// but not fail, it should return an empty InsertBatch. That would ensure that the message is
/// committed but skipped in the processing.
pub type ProcessingFunction = fn(KafkaPayload, KafkaMessageMetadata) -> anyhow::Result<InsertBatch>;

pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
    match name {
        "FunctionsMessageProcessor" => Some(functions::process_message),
        "ProfilesMessageProcessor" => Some(profiles::process_message),
        "QuerylogProcessor" => Some(querylog::process_message),
        "ReplaysProcessor" => Some(replays::process_message),
        "SpansMessageProcessor" => Some(spans::process_message),
        "MetricsSummariesMessageProcessor" => Some(metrics_summaries::process_message),
        "OutcomesProcessor" => Some(outcomes::process_message),
        "GenericCountersMetricsProcessor" => Some(generic_metrics::process_counter_message),
        "GenericSetsMetricsProcessor" => Some(generic_metrics::process_set_message),
        "GenericDistributionsMetricsProcessor" => Some(generic_metrics::process_distribution_message),
        "GenericGaugesMetricsProcessor" => Some(generic_metrics::process_gauge_message),
        _ => None,
    }
}
