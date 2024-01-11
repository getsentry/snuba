mod functions;
mod metrics_summaries;
mod outcomes;
mod profiles;
mod querylog;
mod replays;
mod spans;
mod utils;

use crate::config::ProcessorConfig;
use crate::types::{InsertBatch, KafkaMessageMetadata};
use rust_arroyo::backends::kafka::types::KafkaPayload;

pub type ProcessingFunction =
    fn(KafkaPayload, KafkaMessageMetadata, config: &ProcessorConfig) -> anyhow::Result<InsertBatch>;

macro_rules! define_processing_functions {
    ($(($name:literal, $logical_topic:literal, $function:path)),* $(,)*) => {
        // define function via macro so we can assert statically that processor names are unique.
        // if it weren't for that, it would probably be just as performant to iterate through the
        // PROCESSORS slice since it is const anyway
        pub fn get_processing_function(name: &str) -> Option<ProcessingFunction> {
            match name {
                $($name => Some($function),)*
                _ => None,
            }
        }

        #[cfg(test)]
        const PROCESSORS: &[(&'static str, &'static str, ProcessingFunction)] = &[
            $(($name, $logical_topic, $function),)*
        ];
    };
}

define_processing_functions! {
    // python class name, schema name/logical topic, function path
    ("FunctionsMessageProcessor", "profiles-call-tree", functions::process_message),
    ("ProfilesMessageProcessor", "processed-profiles", profiles::process_message),
    ("QuerylogProcessor", "snuba-queries", querylog::process_message),
    ("ReplaysProcessor", "ingest-replay-events", replays::process_message),
    ("SpansMessageProcessor", "snuba-spans", spans::process_message),
    ("MetricsSummariesMessageProcessor", "snuba-spans", metrics_summaries::process_message),
    ("OutcomesProcessor", "outcomes", outcomes::process_message),
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use chrono::DateTime;
    use sentry_kafka_schemas::get_schema;

    use super::*;

    #[test]
    fn test_schemas() {
        let processor_config = ProcessorConfig::default();
        for (_python_class_name, topic_name, processor_fn) in PROCESSORS {
            let schema = get_schema(topic_name, None).unwrap();
            let metadata = KafkaMessageMetadata {
                partition: 0,
                offset: 1,
                timestamp: DateTime::from(SystemTime::now()),
            };

            for (example_i, example) in schema.examples().iter().enumerate() {
                let mut settings = insta::Settings::clone_current();
                settings.set_snapshot_suffix(format!("{}-{}", topic_name, example_i));

                match *topic_name {
                    "ingest-replay-events" => {
                        settings.add_redaction(".*.event_hash", "<event UUID>");
                    }
                    _ => (),
                }

                settings.set_description(std::str::from_utf8(example).unwrap());
                let _guard = settings.bind_to_scope();

                let payload = KafkaPayload::new(None, None, Some(example.to_vec()));
                let processed = processor_fn(payload, metadata.clone(), &processor_config).unwrap();
                let encoded_rows = String::from_utf8(processed.rows.into_encoded_rows()).unwrap();
                let mut snapshot_payload = Vec::new();
                for row in encoded_rows.lines() {
                    let row_value: serde_json::Value = serde_json::from_str(row).unwrap();
                    snapshot_payload.push(row_value);
                }
                insta::assert_json_snapshot!(snapshot_payload);
            }
        }
    }
}
