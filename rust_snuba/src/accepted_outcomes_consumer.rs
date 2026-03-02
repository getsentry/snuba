use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::metrics;
use sentry_arroyo::processing::dlq::{DlqLimit, DlqPolicy, KafkaDlqProducer};
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::{Partition, Topic};

use pyo3::prelude::*;

use crate::config;
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::global_tags::set_global_tag;
use crate::metrics::statsd::StatsDBackend;
use crate::strategies::accepted_outcomes::aggregator::OutcomesAggregator;
use crate::strategies::accepted_outcomes::commit_outcomes::CommitOutcomes;
use crate::strategies::accepted_outcomes::produce_outcome::ProduceAcceptedOutcome;
use crate::strategies::noop::Noop;

pub struct AcceptedOutcomesStrategyFactory {
    bucket_interval: u64,
    max_batch_time_ms: Duration,
    produce_topic: Topic,
    producer: Arc<KafkaProducer>,
    concurrency: ConcurrencyConfig,
}

impl ProcessingStrategyFactory<KafkaPayload> for AcceptedOutcomesStrategyFactory {
    fn update_partitions(&self, _partitions: &HashMap<Partition, u64>) {
        // No-op for now
    }

    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let produce = ProduceAcceptedOutcome::new(
            Noop,
            self.producer.clone(),
            self.produce_topic,
            &self.concurrency,
            false,
        );
        let commit = CommitOutcomes::new(produce);
        Box::new(OutcomesAggregator::new(
            commit,
            self.bucket_interval,
            self.max_batch_time_ms,
        ))
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn accepted_outcomes_consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    concurrency: usize,
    enforce_schema: bool,
    max_poll_interval_ms: usize,
    health_check: &str,
    health_check_file: Option<&str>,
    max_dlq_buffer_length: Option<usize>,
    join_timeout_ms: Option<u64>,
    max_batch_time_ms: u64,
    bucket_interval: u64,
    produce_topic: &str,
) -> usize {
    py.allow_threads(|| {
        accepted_outcomes_consumer_impl(
            consumer_group,
            auto_offset_reset,
            no_strict_offset_reset,
            consumer_config_raw,
            concurrency,
            enforce_schema,
            max_poll_interval_ms,
            health_check,
            health_check_file,
            max_dlq_buffer_length,
            join_timeout_ms,
            max_batch_time_ms,
            bucket_interval,
            produce_topic,
        )
    })
}

#[allow(clippy::too_many_arguments)]
pub fn accepted_outcomes_consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    concurrency: usize,
    _enforce_schema: bool,
    max_poll_interval_ms: usize,
    _health_check: &str,
    _health_check_file: Option<&str>,
    max_dlq_buffer_length: Option<usize>,
    _join_timeout_ms: Option<u64>,
    max_batch_time_ms: u64,
    bucket_interval: u64,
    produce_topic: &str,
) -> usize {
    setup_logging();

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();

    assert_eq!(consumer_config.storages.len(), 1);

    let mut _sentry_guard = None;

    // setup sentry
    if let Some(dsn) = consumer_config.env.sentry_dsn {
        tracing::debug!(sentry_dsn = dsn);
        std::env::set_var("RUST_BACKTRACE", "1");
        _sentry_guard = Some(setup_sentry(&dsn));
    }

    // setup arroyo metrics
    if let (Some(host), Some(port)) = (
        consumer_config.env.dogstatsd_host,
        consumer_config.env.dogstatsd_port,
    ) {
        let storage_name = consumer_config
            .storages
            .iter()
            .map(|s| s.name.clone())
            .collect::<Vec<_>>()
            .join(",");
        set_global_tag("storage".to_owned(), storage_name);
        set_global_tag("consumer_group".to_owned(), consumer_group.to_owned());

        metrics::init(StatsDBackend::new(&host, port, "snuba.consumer")).unwrap();
    }

    let produce_broker_config = consumer_config.raw_topic.broker_config.clone();

    let config = KafkaConfig::new_consumer_config(
        vec![],
        consumer_group.to_owned(),
        auto_offset_reset.parse().expect(
            "Invalid value for `auto_offset_reset`. Valid values: `error`, `earliest`, `latest`",
        ),
        !no_strict_offset_reset,
        max_poll_interval_ms,
        Some(consumer_config.raw_topic.broker_config),
    );

    // DLQ setup
    let dlq_concurrency_config = ConcurrencyConfig::new(10);

    let dlq_policy = consumer_config.dlq_topic.map(|dlq_topic_config| {
        let producer_config =
            KafkaConfig::new_producer_config(vec![], Some(dlq_topic_config.broker_config));
        let producer = KafkaProducer::new(producer_config);

        let kafka_dlq_producer = Box::new(KafkaDlqProducer::new(
            producer,
            Topic::new(&dlq_topic_config.physical_topic_name),
        ));

        let handle = dlq_concurrency_config.handle();
        DlqPolicy::new(
            handle,
            kafka_dlq_producer,
            DlqLimit {
                max_invalid_ratio: None,
                max_consecutive_count: None,
            },
            max_dlq_buffer_length,
        )
    });

    let topic = Topic::new(&consumer_config.raw_topic.physical_topic_name);

    let produce_topic = Topic::new(produce_topic);
    let producer_config = KafkaConfig::new_producer_config(vec![], Some(produce_broker_config));
    let producer = Arc::new(KafkaProducer::new(producer_config));

    let factory = AcceptedOutcomesStrategyFactory {
        bucket_interval,
        max_batch_time_ms: Duration::from_millis(max_batch_time_ms),
        produce_topic,
        producer,
        concurrency: ConcurrencyConfig::new(concurrency),
    };
    let processor = StreamProcessor::with_kafka(config, factory, topic, dlq_policy);

    let mut handle = processor.get_handle();

    // Set up Ctrl-C handler for graceful shutdown
    ctrlc::set_handler(move || {
        handle.signal_shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    if let Err(error) = processor.run() {
        let error: &dyn std::error::Error = &error;
        tracing::error!("{:?}", error);
        1
    } else {
        0
    }
}
