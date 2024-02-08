use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::metrics;
use rust_arroyo::processing::dlq::{DlqLimit, DlqPolicy, KafkaDlqProducer};

use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::Topic;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::config;
use crate::factory::ConsumerStrategyFactory;
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::global_tags::set_global_tag;
use crate::metrics::statsd::StatsDBackend;
use crate::processors;
use crate::types::{InsertOrReplacement, KafkaMessageMetadata};

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
    enforce_schema: bool,
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
            enforce_schema,
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
    enforce_schema: bool,
    max_poll_interval_ms: usize,
    python_max_queue_depth: Option<usize>,
    health_check_file: Option<&str>,
) -> usize {
    setup_logging();

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
    let max_batch_size = consumer_config.max_batch_size;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    tracing::info!(?consumer_config, "Starting Rust consumer");

    // TODO: Support multiple storages
    assert_eq!(consumer_config.storages.len(), 1);

    let mut _sentry_guard = None;

    let env_config = consumer_config.env.clone();

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
        let storage_name = consumer_config
            .storages
            .iter()
            .map(|s| s.name.clone())
            .collect::<Vec<_>>()
            .join(",");
        set_global_tag("storage".to_owned(), storage_name);
        set_global_tag("consumer_group".to_owned(), consumer_group.to_owned());

        metrics::init(StatsDBackend::new(
            &host,
            port,
            "snuba.consumer",
            env_config.ddm_metrics_sample_rate,
        ))
        .unwrap();
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

    // XXX: this variable must live for the lifetime of the entire consumer. we should do something
    // to ensure this statically, such as use actual Rust lifetimes or ensuring the runtime stays
    // alive by storing it inside of the DlqPolicy
    let dlq_concurrency_config = ConcurrencyConfig::new(10);

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

            let handle = dlq_concurrency_config.handle();
            DlqPolicy::new(
                handle,
                kafka_dlq_producer,
                DlqLimit {
                    max_invalid_ratio: Some(0.01),
                    max_consecutive_count: Some(1000),
                },
                None,
            )
        }),
    };

    let commit_log_producer = if let Some(topic_config) = consumer_config.commit_log_topic {
        let producer_config =
            KafkaConfig::new_producer_config(vec![], Some(topic_config.broker_config));
        let producer = KafkaProducer::new(producer_config);
        Some((
            Arc::new(producer),
            Topic::new(&topic_config.physical_topic_name),
        ))
    } else {
        None
    };

    let replacements_config = if let Some(topic_config) = consumer_config.replacements_topic {
        let producer_config =
            KafkaConfig::new_producer_config(vec![], Some(topic_config.broker_config));
        Some((
            producer_config,
            Topic::new(&topic_config.physical_topic_name),
        ))
    } else {
        None
    };

    let factory = ConsumerStrategyFactory::new(
        first_storage,
        env_config,
        logical_topic_name,
        max_batch_size,
        max_batch_time,
        skip_write,
        ConcurrencyConfig::new(concurrency),
        ConcurrencyConfig::new(2),
        ConcurrencyConfig::new(2),
        ConcurrencyConfig::new(4),
        python_max_queue_depth,
        use_rust_processor,
        health_check_file.map(ToOwned::to_owned),
        enforce_schema,
        commit_log_producer,
        replacements_config,
        consumer_group.to_owned(),
        Topic::new(&consumer_config.raw_topic.physical_topic_name),
        consumer_config.accountant_topic,
    );

    let topic = Topic::new(&consumer_config.raw_topic.physical_topic_name);
    let processor = StreamProcessor::with_kafka(config, factory, topic, dlq_policy);

    let mut handle = processor.get_handle();

    ctrlc::set_handler(move || {
        handle.signal_shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    if let Err(error) = processor.run() {
        let error: &dyn std::error::Error = &error;
        tracing::error!(error);
        1
    } else {
        0
    }
}

pyo3::create_exception!(rust_snuba, SnubaRustError, pyo3::exceptions::PyException);

/// insert: encoded rows
type PyInsert = PyObject;

/// replacement: (key/project_id, value)
type PyReplacement = (PyObject, PyObject);

#[pyfunction]
pub fn process_message(
    py: Python,
    name: &str,
    value: Vec<u8>,
    partition: u16,
    offset: u64,
    millis_since_epoch: i64,
) -> PyResult<(Option<PyInsert>, Option<PyReplacement>)> {
    // XXX: Currently only takes the message payload and metadata. This assumes
    // key and headers are not used for message processing
    let func = processors::get_processing_function(name)
        .ok_or(SnubaRustError::new_err("processor not found"))?;

    let payload = KafkaPayload::new(None, None, Some(value));

    let timestamp = DateTime::from_naive_utc_and_offset(
        NaiveDateTime::from_timestamp_millis(millis_since_epoch).unwrap_or(NaiveDateTime::MIN),
        Utc,
    );

    let meta = KafkaMessageMetadata {
        partition,
        offset,
        timestamp,
    };

    match func {
        processors::ProcessingFunctionType::ProcessingFunction(f) => {
            let res = f(payload, meta, &config::ProcessorConfig::default())
                .map_err(|e| SnubaRustError::new_err(format!("invalid message: {:?}", e)))?;

            let payload = PyBytes::new(py, &res.rows.into_encoded_rows()).into();

            Ok((Some(payload), None))
        }
        processors::ProcessingFunctionType::ProcessingFunctionWithReplacements(f) => {
            let res = f(payload, meta, &config::ProcessorConfig::default())
                .map_err(|e| SnubaRustError::new_err(format!("invalid message: {:?}", e)))?;

            match res {
                InsertOrReplacement::Insert(r) => {
                    let payload = PyBytes::new(py, &r.rows.into_encoded_rows()).into();
                    Ok((Some(payload), None))
                }
                InsertOrReplacement::Replacement(r) => {
                    let key_bytes = PyBytes::new(py, &r.key).into();
                    let value_bytes = PyBytes::new(py, &r.value).into();
                    Ok((None, Some((key_bytes, value_bytes))))
                }
            }
        }
    }
}
