use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;

use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::Topic;
use rust_arroyo::utils::metrics::configure_metrics;

use pyo3::prelude::*;

use crate::config;
use crate::factory::ConsumerStrategyFactory;
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::statsd::StatsDBackend;
use crate::processors;
use crate::types::KafkaMessageMetadata;

#[pyfunction]
pub fn consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    consumer_config_raw: &str,
    skip_write: bool,
    concurrency: usize,
    use_rust_processor: bool,
) {
    py.allow_threads(|| {
        consumer_impl(
            consumer_group,
            auto_offset_reset,
            consumer_config_raw,
            skip_write,
            concurrency,
            use_rust_processor,
        )
    });
}

pub fn consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    consumer_config_raw: &str,
    skip_write: bool,
    concurrency: usize,
    use_rust_processor: bool,
) {
    setup_logging();

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
    let max_batch_size = consumer_config.max_batch_size;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    log::info!("Starting Rust consumer with config: {:?}", consumer_config);

    // TODO: Support multiple storages
    assert_eq!(consumer_config.storages.len(), 1);
    assert!(consumer_config.replacements_topic.is_none());
    assert!(consumer_config.commit_log_topic.is_none());

    let mut _sentry_guard = None;

    // setup sentry
    if let Some(dsn) = consumer_config.env.sentry_dsn {
        log::debug!("Using sentry dsn {:?}", dsn);
        _sentry_guard = Some(setup_sentry(dsn));
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

        configure_metrics(Box::new(StatsDBackend::new(
            &host,
            port,
            "snuba.rust_consumer",
            tags,
        )));
    }

    if !use_rust_processor {
        procspawn::init();
    }

    let first_storage = consumer_config.storages[0].clone();

    log::info!("Starting consumer for {:?}", first_storage.name,);

    let broker_config: HashMap<_, _> = consumer_config
        .raw_topic
        .broker_config
        .iter()
        .filter_map(|(k, v)| {
            let v = v.as_ref()?;
            if v.is_empty() {
                return None;
            }
            Some((k.to_owned(), v.to_owned()))
        })
        .collect();

    let config = KafkaConfig::new_consumer_config(
        vec![],
        consumer_group.to_owned(),
        auto_offset_reset.to_owned(),
        false,
        Some(broker_config),
    );

    let consumer = Arc::new(Mutex::new(KafkaConsumer::new(config)));
    let logical_topic_name = consumer_config.raw_topic.logical_topic_name;
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory::new(
            first_storage,
            logical_topic_name,
            max_batch_size,
            max_batch_time,
            skip_write,
            concurrency,
            use_rust_processor,
        )),
    );

    processor.subscribe(Topic::new(&consumer_config.raw_topic.physical_topic_name));

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
    match processors::get_processing_function(name) {
        None => None,
        Some(func) => {
            let payload = KafkaPayload {
                key: None,
                headers: None,
                payload: Some(value),
            };

            let meta = KafkaMessageMetadata {
                partition,
                offset,
                timestamp: DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::from_timestamp_millis(millis_since_epoch)
                        .unwrap_or(NaiveDateTime::MIN),
                    Utc,
                ),
            };

            let res = func(payload, meta);
            println!("res {:?}", res);
            let row = res.unwrap().rows[0].clone();
            Some(row)
        }
    }
}
