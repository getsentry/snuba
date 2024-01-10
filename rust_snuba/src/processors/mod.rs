mod functions;
mod metrics_summaries;
mod outcomes;
mod profiles;
mod querylog;
mod replays;
mod spans;
mod utils;
mod generic_metrics;

use crate::config::ProcessorConfig;
use crate::types::{InsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;

pub type ProcessingFunction =
    fn(KafkaPayload, KafkaMessageMetadata, config: &ProcessorConfig) -> anyhow::Result<InsertBatch>;

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
