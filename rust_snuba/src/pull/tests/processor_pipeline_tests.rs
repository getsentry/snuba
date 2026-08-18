use rstest::rstest;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::PipelineExt;
use sentry_kafka_schemas::get_schema;

use crate::config::ProcessorConfig;
use crate::processors::{get_processing_function, ProcessingFunctionType};

use super::super::pipeline::{Pipeline, PipelineConfig};
use super::super::test_fixtures::collector::TestCollector;
use super::super::test_fixtures::sources::VecSource;

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

    // Duplicate payloads to ensure enough data for batching
    let payloads: Vec<KafkaPayload> = payloads
        .iter()
        .cycle()
        .take(payloads.len() * 3)
        .map(|kp| {
            KafkaPayload::new(
                kp.key().cloned(),
                kp.headers().cloned(),
                kp.payload().cloned(),
            )
        })
        .collect();

    let source = VecSource::from_payloads(payloads);

    let config = PipelineConfig {
        processor,
        processor_config: ProcessorConfig::default(),
        max_batch_rows: 2,
        max_batch_bytes: u64::MAX,
    };

    let pipeline = Pipeline::build(source, &config);
    let mut collector = TestCollector::new();

    let result = pipeline.stream().run(&mut collector).await;

    assert!(
        result.is_ok(),
        "{processor_name}: pipeline failed: {:?}",
        result.err()
    );

    assert!(
        !collector.batches.is_empty(),
        "{processor_name}: no batches produced"
    );

    for (i, batch) in collector.batches.iter().enumerate() {
        assert!(
            batch.len() <= 2,
            "{processor_name}: batch {i} has {} items, expected at most 2",
            batch.len()
        );
        for (j, insert) in batch.iter().enumerate() {
            assert!(
                insert.rows.num_rows > 0,
                "{processor_name}: batch {i} item {j} has no rows"
            );
        }
    }
}
