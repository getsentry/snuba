use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::dlq::{DlqLimit, DlqPolicy, KafkaDlqProducer};

use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{BrokerMessage, Partition, Topic};
use rust_arroyo::utils::metrics::configure_metrics;

use pyo3::prelude::*;

use crate::config;
use crate::factory::ConsumerStrategyFactory;
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::statsd::StatsDBackend;
use crate::processors;
use crate::types::BytesInsertBatch;

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    skip_write: bool,
    concurrency: usize,
    use_rust_processor: bool,
    max_poll_interval_ms: usize,
    python_max_queue_depth: Option<usize>,
    health_check_file: Option<&str>,
) {
    py.allow_threads(|| {
        consumer_impl(
            consumer_group,
            auto_offset_reset,
            no_strict_offset_reset,
            consumer_config_raw,
            skip_write,
            concurrency,
            use_rust_processor,
            max_poll_interval_ms,
            python_max_queue_depth,
            health_check_file,
        )
    });
}

#[allow(clippy::too_many_arguments)]
pub fn consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    skip_write: bool,
    concurrency: usize,
    use_rust_processor: bool,
    max_poll_interval_ms: usize,
    python_max_queue_depth: Option<usize>,
    health_check_file: Option<&str>,
) {
    setup_logging();

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
    let max_batch_size = consumer_config.max_batch_size;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    tracing::info!(?consumer_config, "Starting Rust consumer");

    // TODO: Support multiple storages
    assert_eq!(consumer_config.storages.len(), 1);
    assert!(consumer_config.replacements_topic.is_none());
    assert!(consumer_config.commit_log_topic.is_none());

    let mut _sentry_guard = None;

    // setup sentry
    if let Some(dsn) = consumer_config.env.sentry_dsn {
        tracing::debug!(sentry_dsn = dsn);
        // this forces anyhow to record stack traces when capturing an error:
        std::env::set_var("RUST_BACKTRACE", "1");
        _sentry_guard = Some(setup_sentry(&dsn));
    }

    // setup arroyo metrics
    if let (Some(host), Some(port)) = (
        consumer_config.env.dogstatsd_host,
        consumer_config.env.dogstatsd_port,
    ) {
        let mut tags = HashMap::new();
        let storage_name = consumer_config
            .storages
            .iter()
            .map(|s| s.name.clone())
            .collect::<Vec<_>>()
            .join(",");
        tags.insert("storage", storage_name.as_str());
        tags.insert("consumer_group", consumer_group);

        configure_metrics(StatsDBackend::new(&host, port, "snuba.consumer", tags));
    }

    if !use_rust_processor {
        procspawn::init();
    }

    let first_storage = consumer_config.storages[0].clone();

    tracing::info!(
        storage = first_storage.name,
        "Starting consumer for {:?}",
        first_storage.name,
    );

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

    let logical_topic_name = consumer_config.raw_topic.logical_topic_name;

    // DLQ policy applies only if we are not skipping writes, otherwise we don't want to be
    // writing to the DLQ topics in prod.
    let dlq_policy = match skip_write {
        true => None,
        false => consumer_config.dlq_topic.map(|dlq_topic_config| {
            let producer_config =
                KafkaConfig::new_producer_config(vec![], Some(dlq_topic_config.broker_config));
            let producer = KafkaProducer::new(producer_config);

            let kafka_dlq_producer = Box::new(KafkaDlqProducer::new(
                producer,
                Topic::new(&dlq_topic_config.physical_topic_name),
            ));

            DlqPolicy::new(
                kafka_dlq_producer,
                DlqLimit {
                    max_invalid_ratio: Some(0.01),
                    max_consecutive_count: Some(1000),
                },
                None,
            )
        }),
    };

    let factory = ConsumerStrategyFactory::new(
        first_storage,
        logical_topic_name,
        max_batch_size,
        max_batch_time,
        skip_write,
        ConcurrencyConfig::new(concurrency),
        ConcurrencyConfig::new(2),
        python_max_queue_depth,
        use_rust_processor,
        health_check_file.map(ToOwned::to_owned),
    );

    let topic = Topic::new(&consumer_config.raw_topic.physical_topic_name);
    let processor = StreamProcessor::with_kafka(config, factory, topic, dlq_policy);

    let mut handle = processor.get_handle();

    ctrlc::set_handler(move || {
        handle.signal_shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    processor.run().unwrap();
}

#[pyfunction]
pub fn process_message(
    name: &str,
    value: Vec<u8>,
    partition: u16,
    offset: u64,
    millis_since_epoch: i64,
) -> Option<Vec<u8>> {
    // XXX: Currently only takes the message payload and metadata. This assumes
    // key and headers are not used for message processing
    processors::get_processing_function(name).map(|func| {
        let payload = KafkaPayload::new(None, None, Some(value));

        let timestamp = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_millis(millis_since_epoch).unwrap_or(NaiveDateTime::MIN),
            Utc,
        );
        let partition = Partition::new(Topic::new("_py"), partition);
        let message = BrokerMessage::new(payload, partition, offset, timestamp);

        let res = func(message, None).unwrap();
        let batch = BytesInsertBatch::new(
            res.rows,
            timestamp,
            res.origin_timestamp,
            res.sentry_received_timestamp,
            BTreeMap::from([(partition.index, (offset, timestamp))]),
        );
        batch.encoded_rows().to_vec()
    })
}
