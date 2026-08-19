use std::sync::Arc;
use std::time::Duration;

use rstest::rstest;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{BatchStage, PipelineExt};
use sentry_kafka_schemas::get_schema;

use crate::config::ProcessorConfig;
use crate::processors::{get_processing_function, ProcessingFunctionType};
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::processor_stage::ProcessorStage;
use crate::pull::test_fixtures::writer::MockWriter;

use super::super::pipeline::Pipeline;
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

    let writer = Arc::new(MockWriter::new());

    let pipeline = Pipeline::new(
        VecSource::from_payloads(payloads),
        ProcessorStage::new(processor, ProcessorConfig::default()),
        BatchStage::new(PipelineBatchBuffer::new(), 2, u64::MAX),
        Some(Duration::from_secs(2)), // max_batch_time
        None,                         // idle_timeout
        ClickHouseWriterStage::new(Arc::clone(&writer)),
        2,
    );

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

    for (i, batch_meta) in collector.batches.iter().enumerate() {
        // Verify commit log offsets are populated
        assert!(
            !batch_meta.commit_log_offsets.0.is_empty(),
            "{processor_name}: batch {i} has no commit log offsets"
        );
    }

    // Verify the writer was called
    let write_calls = writer.calls();
    assert!(
        !write_calls.is_empty(),
        "{processor_name}: writer was never called"
    );
    for (i, call) in write_calls.iter().enumerate() {
        assert!(
            call.raw_bytes > 0,
            "{processor_name}: write call {i} had 0 bytes"
        );
    }
}
