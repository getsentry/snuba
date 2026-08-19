use std::sync::Arc;
use std::time::Duration;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{
    BatchStage, DlqHandler, Pipeline, PipelineExt, PullSource,
};
use sentry_arroyo::types::{Topic, TopicOrPartition};
use sentry_kafka_schemas::get_schema;

use crate::config::ProcessorConfig;
use crate::processors::{get_processing_function, ProcessingFunctionType};
use crate::pull::batch::buffer::PipelineBatchBuffer;
use crate::pull::pipelines::eap::EapPipeline;
use crate::pull::stages::clickhouse_writer_stage::ClickHouseWriterStage;
use crate::pull::stages::cogs_stage::CogsStage;
use crate::pull::stages::commit_log_stage::CommitLogStage;
use crate::pull::stages::processor_stage::ProcessorStage;
use crate::pull::test_fixtures::collector::TestCollector;
use crate::pull::test_fixtures::producer::MockProducer;
use crate::pull::test_fixtures::sources::VecSource;
use crate::pull::test_fixtures::writer::MockWriter;

#[tokio::test]
async fn test_eap_pipeline() {
    let processor =
        match get_processing_function("EAPItemsProcessor").expect("EAPItemsProcessor not found") {
            ProcessingFunctionType::ProcessingFunction(f) => f,
            ProcessingFunctionType::ProcessingFunctionWithReplacements(_) => {
                panic!("EAPItemsProcessor is a replacement processor — unexpected")
            }
        };

    let schema = get_schema("snuba-items", None).unwrap();
    let payloads: Vec<KafkaPayload> = schema
        .examples()
        .iter()
        .map(|ex| KafkaPayload::new(None, None, Some(ex.payload().to_vec())))
        .collect();

    assert!(!payloads.is_empty(), "no examples for snuba-items");

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
    let writer = Arc::new(MockWriter::new());
    let (dlq_producer, _dlq_calls) = MockProducer::new();
    let (commit_log_producer, commit_log_calls) = MockProducer::new();
    let (cogs_producer, _cogs_calls) = MockProducer::new();

    let pipeline = EapPipeline::new(
        ProcessorStage::new(processor, ProcessorConfig::default()),
        1, // processing_concurrency
        DlqHandler::new(
            dlq_producer,
            TopicOrPartition::Topic(Topic::new("snuba-dead-letter-items")),
        ),
        BatchStage::new(PipelineBatchBuffer::new(), 2, u64::MAX),
        Some(Duration::from_secs(2)),
        None,
        ClickHouseWriterStage::new(Arc::clone(&writer)),
        2,
        CommitLogStage::new(
            commit_log_producer,
            Topic::new("snuba-items-commit-log"),
            Topic::new("snuba-items"),
            "test-group".to_string(),
        ),
        CogsStage::new(
            cogs_producer,
            Topic::new("shared-resources-usage"),
            "eap_items_processor".to_string(),
        ),
    );

    let mut collector = TestCollector::new();
    let result = pipeline.stream(source.stream()).run(&mut collector).await;

    assert!(result.is_ok(), "pipeline failed: {:?}", result.err());
    assert!(!collector.batches.is_empty(), "no batches produced");

    // Verify writer was called
    let write_calls = writer.calls();
    assert!(!write_calls.is_empty(), "writer was never called");

    // Verify commit log was produced
    let cl_calls = commit_log_calls.lock().unwrap();
    assert!(!cl_calls.is_empty(), "commit log producer was never called");
    for (i, call) in cl_calls.iter().enumerate() {
        assert!(call.key.is_some(), "commit log call {i} has no key");
        let key = String::from_utf8(call.key.clone().unwrap()).unwrap();
        assert!(
            key.starts_with("snuba-items:"),
            "commit log key should start with topic name, got: {key}"
        );
    }
}
