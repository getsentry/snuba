use std::sync::{Arc, Mutex};
use std::time::Duration;

use rstest::rstest;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{OffsetTracker, PipelineExt, PullSource};
use sentry_kafka_schemas::get_schema;

use crate::config::ProcessorConfig;
use crate::processors::{get_processing_function, ProcessingFunctionType};

use super::super::stages::processor_stage::ProcessorStage;
use super::super::test_fixtures::sources::VecSource;
use super::super::test_fixtures::stages::CollectStage;

#[rstest]
#[case::functions("FunctionsMessageProcessor", "profiles-call-tree")]
#[case::profiles("ProfilesMessageProcessor", "processed-profiles")]
#[case::querylog("QuerylogProcessor", "snuba-queries")]
#[case::replays("ReplaysProcessor", "ingest-replay-events")]
#[case::outcomes("OutcomesProcessor", "outcomes")]
#[case::generic_metrics("GenericCountersMetricsProcessor", "snuba-generic-metrics")]
#[case::polymorphic_metrics("PolymorphicMetricsProcessor", "snuba-metrics")]
#[case::profile_chunks("ProfileChunksProcessor", "snuba-profile-chunks")]
#[case::eap_items("EAPItemsProcessor", "snuba-items")]
#[case::llm_proxy_cost("LlmProxyCostProcessor", "snuba-llm-proxy-cost")]
#[tokio::test]
async fn test_pull_pipeline(#[case] processor_name: &str, #[case] topic: &str) {
    let config = ProcessorConfig::default();
    let processor = match get_processing_function(processor_name)
        .unwrap_or_else(|| panic!("Unknown processor: {processor_name}"))
    {
        ProcessingFunctionType::ProcessingFunction(f) => f,
        ProcessingFunctionType::ProcessingFunctionWithReplacements(_) => {
            panic!("{processor_name} is a replacement processor — not supported yet")
        }
    };

    let schema = get_schema(topic, None).unwrap();
    let payloads: Vec<KafkaPayload> = schema
        .examples()
        .iter()
        .map(|ex| KafkaPayload::new(None, None, Some(ex.payload().to_vec())))
        .collect();

    assert!(
        !payloads.is_empty(),
        "{processor_name}: no examples for topic {topic}"
    );

    let num_payloads = payloads.len();
    let source = VecSource::from_payloads(payloads);
    let processor_stage = ProcessorStage::new(processor, config.clone());
    let (collector, collected) = CollectStage::new();
    let mut tracker = OffsetTracker::new(Duration::from_millis(1), source.committer());

    let result = source
        .stream()
        .apply(&processor_stage)
        .apply(&collector)
        .commit(&mut tracker)
        .await;

    assert!(result.is_ok(), "Pipeline failed: {:?}", result.err());
    let batches = collected.lock().unwrap();

    assert!(
        !batches.is_empty(),
        "{processor_name}: produced no output from {num_payloads} examples"
    );

    for (i, batch) in batches.iter().enumerate() {
        assert!(
            batch.rows.num_rows > 0,
            "{processor_name}: batch {i} has no rows"
        );
    }
}
