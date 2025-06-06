mod eap_items;
pub(crate) mod eap_items_span;
mod errors;
mod functions;
mod generic_metrics;
mod outcomes;
mod profile_chunks;
mod profiles;
mod querylog;
mod release_health_metrics;
mod replays;
mod spans;
mod uptime_monitor_checks;
mod utils;

use crate::config::ProcessorConfig;
use crate::types::{InsertBatch, InsertOrReplacement, KafkaMessageMetadata};
use sentry_arroyo::backends::kafka::types::KafkaPayload;

pub enum ProcessingFunctionType {
    ProcessingFunction(ProcessingFunction),
    ProcessingFunctionWithReplacements(ProcessingFunctionWithReplacements),
}

pub type ProcessingFunction =
    fn(KafkaPayload, KafkaMessageMetadata, config: &ProcessorConfig) -> anyhow::Result<InsertBatch>;

pub type ProcessingFunctionWithReplacements =
    fn(
        KafkaPayload,
        KafkaMessageMetadata,
        config: &ProcessorConfig,
    ) -> anyhow::Result<InsertOrReplacement<InsertBatch>>;

macro_rules! define_processing_functions {
    ($(($name:literal, $logical_topic:literal, $function:expr)),* $(,)*) => {
        // define function via macro so we can assert statically that processor names are unique.
        // if it weren't for that, it would probably be just as performant to iterate through the
        // PROCESSORS slice since it is const anyway
        pub fn get_processing_function(name: &str) -> Option<ProcessingFunctionType> {
            match name {
                $($name => Some($function),)*
                _ => None,
            }
        }

        pub const PROCESSORS: &[(&'static str, &'static str, ProcessingFunctionType)] = &[
            $(($name, $logical_topic, $function),)*
        ];
    };
}

define_processing_functions! {
    // python class name, schema name/logical topic, function path
    ("FunctionsMessageProcessor", "profiles-call-tree", ProcessingFunctionType::ProcessingFunction(functions::process_message)),
    ("ProfilesMessageProcessor", "processed-profiles", ProcessingFunctionType::ProcessingFunction(profiles::process_message)),
    ("QuerylogProcessor", "snuba-queries", ProcessingFunctionType::ProcessingFunction(querylog::process_message)),
    ("ReplaysProcessor", "ingest-replay-events", ProcessingFunctionType::ProcessingFunction(replays::process_message)),
    ("UptimeMonitorChecksProcessor", "snuba-uptime-results", ProcessingFunctionType::ProcessingFunction(uptime_monitor_checks::process_message)),
    ("SpansMessageProcessor", "snuba-spans", ProcessingFunctionType::ProcessingFunction(spans::process_message)),
    ("OutcomesProcessor", "outcomes", ProcessingFunctionType::ProcessingFunction(outcomes::process_message)),
    ("GenericCountersMetricsProcessor", "snuba-generic-metrics", ProcessingFunctionType::ProcessingFunction(generic_metrics::process_counter_message)),
    ("GenericSetsMetricsProcessor", "snuba-generic-metrics", ProcessingFunctionType::ProcessingFunction(generic_metrics::process_set_message)),
    ("GenericDistributionsMetricsProcessor" , "snuba-generic-metrics", ProcessingFunctionType::ProcessingFunction(generic_metrics::process_distribution_message)),
    ("GenericGaugesMetricsProcessor", "snuba-generic-metrics", ProcessingFunctionType::ProcessingFunction(generic_metrics::process_gauge_message)),
    ("PolymorphicMetricsProcessor", "snuba-metrics", ProcessingFunctionType::ProcessingFunction(release_health_metrics::process_metrics_message)),
    ("ErrorsProcessor", "events", ProcessingFunctionType::ProcessingFunctionWithReplacements(errors::process_message_with_replacement)),
    ("ProfileChunksProcessor", "snuba-profile-chunks", ProcessingFunctionType::ProcessingFunction(profile_chunks::process_message)),
    ("EAPItemsProcessor", "snuba-items", ProcessingFunctionType::ProcessingFunction(eap_items::process_message)),
    ("EAPItemsSpanProcessor", "snuba-spans", ProcessingFunctionType::ProcessingFunction(eap_items_span::process_message)),
}

// COGS is recorded for these processors
pub fn get_cogs_label(processor_name: &str) -> Option<String> {
    match processor_name {
        "GenericCountersMetricsProcessor" => Some("generic_metrics_processor_counters".to_string()),
        "GenericSetsMetricsProcessor" => Some("generic_metrics_processor_sets".to_string()),
        "GenericDistributionsMetricsProcessor" => {
            Some("generic_metrics_processor_distributions".to_string())
        }
        "GenericGaugesMetricsProcessor" => Some("generic_metrics_processor_gauges".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use chrono::DateTime;
    use schemars::JsonSchema;
    use sentry_kafka_schemas::get_schema;

    use super::*;

    pub fn run_schema_type_test<M: JsonSchema>(schema_name: &str, subschema: Option<&str>) {
        let schema = schemars::schema_for!(M);

        let old_schema = sentry_kafka_schemas::get_schema(schema_name, None).unwrap();
        let mut old_schema: serde_json::Value =
            serde_json::from_str(old_schema.raw_schema()).unwrap();
        if let Some(subschema) = subschema {
            let definitions = old_schema
                .as_object()
                .unwrap()
                .get("definitions")
                .unwrap()
                .as_object()
                .unwrap()
                .clone();

            old_schema = definitions.get(subschema).unwrap().clone();

            old_schema
                .as_object_mut()
                .unwrap()
                .insert("definitions".to_owned(), definitions.into());
        }

        println!("{}", serde_json::to_string(&schema).unwrap());

        let mut diff =
            json_schema_diff::diff(old_schema, serde_json::to_value(schema).unwrap()).unwrap();
        diff.retain(|change| change.change.is_breaking());
        if !diff.is_empty() {
            insta::assert_debug_snapshot!(
                format!("{}-{}", schema_name, subschema.unwrap_or("")),
                diff
            );
        }
    }

    #[test]
    fn test_schemas() {
        let processor_config = ProcessorConfig::default();
        for (python_class_name, topic_name, processor_fn_type) in PROCESSORS {
            let schema = get_schema(topic_name, None).unwrap();
            let metadata = KafkaMessageMetadata {
                partition: 0,
                offset: 1,
                timestamp: DateTime::from(SystemTime::now()),
            };

            for example in schema.examples() {
                let mut settings = insta::Settings::clone_current();
                settings.set_snapshot_suffix(format!(
                    "{}-{}-{}",
                    topic_name,
                    python_class_name,
                    example.name()
                ));

                if *topic_name == "ingest-replay-events" {
                    settings.add_redaction(".*.event_hash", "<event UUID>");
                }

                if *topic_name == "events" {
                    settings.add_redaction(".*.message_timestamp", "<event timestamp>");
                }

                // This payload is protobuf (so binary), not JSON (so text).
                if *topic_name != "snuba-items" {
                    settings.set_description(std::str::from_utf8(example.payload()).unwrap());
                }

                let _guard = settings.bind_to_scope();

                let payload = KafkaPayload::new(None, None, Some(example.payload().to_vec()));

                match processor_fn_type {
                    ProcessingFunctionType::ProcessingFunction(processor_fn) => {
                        let processed =
                            processor_fn(payload, metadata.clone(), &processor_config).unwrap();
                        let encoded_rows =
                            String::from_utf8(processed.rows.into_encoded_rows()).unwrap();
                        let mut snapshot_payload = Vec::new();
                        for row in encoded_rows.lines() {
                            let row_value: serde_json::Value = serde_json::from_str(row).unwrap();
                            snapshot_payload.push(row_value);
                        }
                        insta::assert_json_snapshot!(snapshot_payload);
                    }
                    ProcessingFunctionType::ProcessingFunctionWithReplacements(processor_fn) => {
                        let processed =
                            processor_fn(payload, metadata.clone(), &processor_config).unwrap();

                        match processed {
                            InsertOrReplacement::Insert(insert) => {
                                let encoded_rows =
                                    String::from_utf8(insert.rows.into_encoded_rows()).unwrap();
                                let mut snapshot_payload = Vec::new();
                                for row in encoded_rows.lines() {
                                    let row_value: serde_json::Value =
                                        serde_json::from_str(row).unwrap();
                                    snapshot_payload.push(row_value);
                                }
                                insta::assert_json_snapshot!(snapshot_payload);
                            }
                            InsertOrReplacement::Replacement(_replacement) => {}
                        }
                    }
                }
            }
        }
    }
}
