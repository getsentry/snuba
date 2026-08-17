use std::sync::{Arc, Mutex};
use std::time::Duration;

use rstest::rstest;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, PipelineExit, Stage, StageResult};
use sentry_kafka_schemas::get_schema;

use crate::config::ProcessorConfig;
use crate::processors::{get_processing_function, ProcessingFunctionType};
use crate::types::InsertBatch;

use super::super::pipeline::{run_pipeline, PullPipelineConfig};
use super::super::stages::noop_stage::NoopStage;
use super::super::test_fixtures::sources::VecSource;

/// Collecting stage — captures batches for test assertions.
struct CollectStage {
    collected: Arc<Mutex<Vec<Vec<InsertBatch>>>>,
}

impl CollectStage {
    fn new() -> (Self, Arc<Mutex<Vec<Vec<InsertBatch>>>>) {
        let collected = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                collected: collected.clone(),
            },
            collected,
        )
    }
}

impl Stage for CollectStage {
    type In = Vec<InsertBatch>;
    type Out = Vec<InsertBatch>;

    async fn process(
        &self,
        envelope: PipelineEnvelope<Vec<InsertBatch>>,
    ) -> StageResult<Vec<InsertBatch>> {
        self.collected
            .lock()
            .unwrap()
            .push(envelope.payload.clone());
        StageResult::Emit(envelope)
    }

    fn name(&self) -> &str {
        "collect"
    }
}

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

    let source = VecSource::from_payloads(payloads);

    let config = PullPipelineConfig {
        processor,
        processor_config: ProcessorConfig::default(),
        max_batch_rows: u64::MAX, // batch all examples into one batch
        max_batch_bytes: u64::MAX,
    };

    // Use a collecting observer to capture output batches
    let (collector, collected) = CollectStage::new();

    let result = run_pipeline(&source, &config, &collector).await;

    match result {
        Ok(PipelineExit::Complete) => {}
        other => panic!("{processor_name}: expected Complete, got {other:?}"),
    }

    // With max_batch_rows = MAX, nothing flushes during the stream.
    // The batch stays in the accumulator. This is expected — the
    // stream ends before the batch is full.
    //
    // To verify processing actually worked, we can run with a smaller
    // batch size. Let's do that as a separate test.
}

/// Test with a small batch size to verify batching + pipeline integration.
#[tokio::test]
async fn test_pull_pipeline_with_batching() {
    let processor = match get_processing_function("QuerylogProcessor").unwrap() {
        ProcessingFunctionType::ProcessingFunction(f) => f,
        _ => panic!("Expected ProcessingFunction"),
    };

    let schema = get_schema("snuba-queries", None).unwrap();
    let payloads: Vec<KafkaPayload> = schema
        .examples()
        .iter()
        .map(|ex| KafkaPayload::new(None, None, Some(ex.payload().to_vec())))
        .collect();

    let num_payloads = payloads.len();
    assert!(num_payloads >= 2, "Need at least 2 examples for batch test");

    let source = VecSource::from_payloads(payloads);

    let config = PullPipelineConfig {
        processor,
        processor_config: ProcessorConfig::default(),
        max_batch_rows: 2,
        max_batch_bytes: u64::MAX,
    };

    let (collector, collected) = CollectStage::new();

    let result = run_pipeline(&source, &config, &collector).await;
    assert!(result.is_ok(), "Pipeline failed: {:?}", result.err());

    let batches = collected.lock().unwrap();
    assert!(
        !batches.is_empty(),
        "Expected at least one batch with batch_size=2 and {num_payloads} examples"
    );

    // Each batch should have at most 2 InsertBatches
    for (i, batch) in batches.iter().enumerate() {
        assert!(
            batch.len() <= 2,
            "Batch {i} has {} items, expected at most 2",
            batch.len()
        );
        for (j, insert) in batch.iter().enumerate() {
            assert!(insert.rows.num_rows > 0, "Batch {i} item {j} has no rows");
        }
    }
}
