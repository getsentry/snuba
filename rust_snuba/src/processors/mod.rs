mod functions;
mod metrics_summaries;
mod profiles;
mod querylog;
mod replays;
mod spans;
mod utils;

use crate::types::InsertBatch;
use anyhow::Context as _;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::types::BrokerMessage;

pub type ProcessingFunction =
    fn(BrokerMessage<KafkaPayload>, Option<serde_json::Value>) -> anyhow::Result<InsertBatch>;

pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
    match name {
        "FunctionsMessageProcessor" => Some(process_functions),
        "ProfilesMessageProcessor" => Some(process_profiles),
        "QuerylogProcessor" => Some(process_querylog),
        "ReplaysProcessor" => Some(process_replays),
        "SpansMessageProcessor" => Some(process_spans),
        "MetricsSummariesMessageProcessor" => Some(process_metrics_summaries),
        _ => None,
    }
}

macro_rules! mk_processing_fn {
    ($name:ident, $fn:path) => {
        fn $name(
            msg: BrokerMessage<KafkaPayload>,
            maybe_value: Option<serde_json::Value>,
        ) -> anyhow::Result<InsertBatch> {
            let value = if let Some(value) = maybe_value {
                serde_json::from_value(value)
            } else {
                let payload_bytes = msg.payload.payload().context("Expected payload")?;
                serde_json::from_slice(payload_bytes)
            }?;
            $fn(value, msg)
        }
    };
}

mk_processing_fn!(process_functions, functions::process_message);
mk_processing_fn!(process_profiles, profiles::process_message);
mk_processing_fn!(process_querylog, querylog::process_message);
mk_processing_fn!(process_replays, replays::process_message);
mk_processing_fn!(process_spans, spans::process_message);
mk_processing_fn!(
    process_metrics_summaries,
    metrics_summaries::process_message
);

#[cfg(test)]
fn make_test_message<T>(raw_msg: &str) -> (T, BrokerMessage<KafkaPayload>)
where
    T: for<'a> serde::Deserialize<'a>,
{
    use rust_arroyo::types::{Partition, Topic};

    let value = serde_json::from_str(raw_msg).unwrap();
    let payload = KafkaPayload::new(None, None, Some(raw_msg.as_bytes().to_vec()));
    let partition = Partition::new(Topic::new("test"), 0);
    let msg = rust_arroyo::types::BrokerMessage::new(
        payload,
        partition,
        1,
        chrono::DateTime::from(std::time::SystemTime::now()),
    );

    (value, msg)
}
