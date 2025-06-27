use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::metrics;
use sentry_arroyo::processing::dlq::{DlqLimit, DlqPolicy, KafkaDlqProducer};

use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::Topic;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::config;
use crate::factory::ConsumerStrategyFactory;
use crate::factory_v2::ConsumerStrategyFactoryV2;
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::global_tags::set_global_tag;
use crate::metrics::statsd::StatsDBackend;
use crate::processors;
use crate::rebalancing;
use crate::types::{InsertOrReplacement, KafkaMessageMetadata};

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    concurrency: usize,
    clickhouse_concurrency: usize,
    use_rust_processor: bool,
    enforce_schema: bool,
    max_poll_interval_ms: usize,
    async_inserts: bool,
    health_check: &str,
    python_max_queue_depth: Option<usize>,
    health_check_file: Option<&str>,
    stop_at_timestamp: Option<i64>,
    batch_write_timeout_ms: Option<u64>,
    max_dlq_buffer_length: Option<usize>,
    custom_envoy_request_timeout: Option<u64>,
    join_timeout_ms: Option<u64>,
    consumer_version: Option<&str>,
) -> usize {
    if consumer_version == Some("v2") {
        py.allow_threads(|| {
            consumer_v2_impl(
                consumer_group,
                auto_offset_reset,
                no_strict_offset_reset,
                consumer_config_raw,
                concurrency,
                clickhouse_concurrency,
                use_rust_processor,
                enforce_schema,
                max_poll_interval_ms,
                async_inserts,
                python_max_queue_depth,
                health_check_file,
                stop_at_timestamp,
                batch_write_timeout_ms,
                max_dlq_buffer_length,
                custom_envoy_request_timeout,
                join_timeout_ms,
                health_check,
            )
        })
    } else {
        py.allow_threads(|| {
            consumer_impl(
                consumer_group,
                auto_offset_reset,
                no_strict_offset_reset,
                consumer_config_raw,
                concurrency,
                clickhouse_concurrency,
                use_rust_processor,
                enforce_schema,
                max_poll_interval_ms,
                async_inserts,
                python_max_queue_depth,
                health_check_file,
                stop_at_timestamp,
                batch_write_timeout_ms,
                max_dlq_buffer_length,
                custom_envoy_request_timeout,
                join_timeout_ms,
                health_check,
            )
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub fn consumer_v2_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    concurrency: usize,
    clickhouse_concurrency: usize,
    use_rust_processor: bool,
    enforce_schema: bool,
    max_poll_interval_ms: usize,
    async_inserts: bool,
    python_max_queue_depth: Option<usize>,
    health_check_file: Option<&str>,
    stop_at_timestamp: Option<i64>,
    batch_write_timeout_ms: Option<u64>,
    max_dlq_buffer_length: Option<usize>,
    custom_envoy_request_timeout: Option<u64>,
    join_timeout_ms: Option<u64>,
    health_check: &str,
) -> usize {
    setup_logging();

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
    let max_batch_size = consumer_config.max_batch_size;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    let batch_write_timeout = match batch_write_timeout_ms {
        Some(timeout_ms) => {
            if timeout_ms >= consumer_config.max_batch_time_ms {
                Some(Duration::from_millis(timeout_ms))
            } else {
                None
            }
        }
        None => None,
    };

    for storage in &consumer_config.storages {
        tracing::info!(
            "Storage: {}, ClickHouse Table Name: {}, Message Processor: {:?}, ClickHouse host: {}, ClickHouse port: {}, ClickHouse HTTP port: {}, ClickHouse database: {}",
            storage.name,
            storage.clickhouse_table_name,
            &storage.message_processor,
            storage.clickhouse_cluster.host,
            storage.clickhouse_cluster.port,
            storage.clickhouse_cluster.http_port,
            storage.clickhouse_cluster.database,
        );
    }

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

        metrics::init(StatsDBackend::new(&host, port, "snuba.consumer")).unwrap();
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

    let topic = Topic::new(&consumer_config.raw_topic.physical_topic_name);

    let mut rebalance_delay_secs = consumer_config
        .raw_topic
        .quantized_rebalance_consumer_group_delay_secs;
    let config_rebalance_delay_secs = rebalancing::get_rebalance_delay_secs(consumer_group);
    if let Some(secs) = config_rebalance_delay_secs {
        rebalance_delay_secs = Some(secs);
    }
    if let Some(secs) = rebalance_delay_secs {
        rebalancing::delay_kafka_rebalance(secs)
    }

    let factory = ConsumerStrategyFactoryV2 {
        storage_config: first_storage,
        env_config,
        logical_topic_name,
        max_batch_size,
        max_batch_time,
        processing_concurrency: ConcurrencyConfig::new(concurrency),
        clickhouse_concurrency: ConcurrencyConfig::new(clickhouse_concurrency),
        commitlog_concurrency: ConcurrencyConfig::new(2),
        replacements_concurrency: ConcurrencyConfig::new(4),
        async_inserts,
        python_max_queue_depth,
        use_rust_processor,
        health_check_file: health_check_file.map(ToOwned::to_owned),
        enforce_schema,
        commit_log_producer,
        replacements_config,
        physical_consumer_group: consumer_group.to_owned(),
        physical_topic_name: Topic::new(&consumer_config.raw_topic.physical_topic_name),
        accountant_topic_config: consumer_config.accountant_topic,
        stop_at_timestamp,
        batch_write_timeout,
        custom_envoy_request_timeout,
        join_timeout_ms,
        health_check: health_check.to_string(),
    };

    let processor = StreamProcessor::with_kafka(config, factory, topic, dlq_policy);

    let mut handle = processor.get_handle();

    match rebalance_delay_secs {
        Some(secs) => {
            ctrlc::set_handler(move || {
                rebalancing::delay_kafka_rebalance(secs);
                handle.signal_shutdown();
            })
            .expect("Error setting Ctrl-C handler");
        }
        None => {
            ctrlc::set_handler(move || {
                handle.signal_shutdown();
            })
            .expect("Error setting Ctrl-C handler");
        }
    }

    if let Err(error) = processor.run() {
        let error: &dyn std::error::Error = &error;
        tracing::error!("{:?}", error);
        1
    } else {
        0
    }
}

#[allow(clippy::too_many_arguments)]
pub fn consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    concurrency: usize,
    clickhouse_concurrency: usize,
    use_rust_processor: bool,
    enforce_schema: bool,
    max_poll_interval_ms: usize,
    async_inserts: bool,
    python_max_queue_depth: Option<usize>,
    health_check_file: Option<&str>,
    stop_at_timestamp: Option<i64>,
    batch_write_timeout_ms: Option<u64>,
    max_dlq_buffer_length: Option<usize>,
    custom_envoy_request_timeout: Option<u64>,
    join_timeout_ms: Option<u64>,
    health_check: &str,
) -> usize {
    setup_logging();

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
    let max_batch_size = consumer_config.max_batch_size;
    let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);

    let batch_write_timeout = match batch_write_timeout_ms {
        Some(timeout_ms) => {
            if timeout_ms >= consumer_config.max_batch_time_ms {
                Some(Duration::from_millis(timeout_ms))
            } else {
                None
            }
        }
        None => None,
    };

    for storage in &consumer_config.storages {
        tracing::info!(
            "Storage: {}, ClickHouse Table Name: {}, Message Processor: {:?}, ClickHouse host: {}, ClickHouse port: {}, ClickHouse HTTP port: {}, ClickHouse database: {}",
            storage.name,
            storage.clickhouse_table_name,
            &storage.message_processor,
            storage.clickhouse_cluster.host,
            storage.clickhouse_cluster.port,
            storage.clickhouse_cluster.http_port,
            storage.clickhouse_cluster.database,
        );
    }

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

        metrics::init(StatsDBackend::new(&host, port, "snuba.consumer")).unwrap();
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

    let topic = Topic::new(&consumer_config.raw_topic.physical_topic_name);

    let mut rebalance_delay_secs = consumer_config
        .raw_topic
        .quantized_rebalance_consumer_group_delay_secs;
    let config_rebalance_delay_secs = rebalancing::get_rebalance_delay_secs(consumer_group);
    if let Some(secs) = config_rebalance_delay_secs {
        rebalance_delay_secs = Some(secs);
    }
    if let Some(secs) = rebalance_delay_secs {
        rebalancing::delay_kafka_rebalance(secs)
    }

    let factory = ConsumerStrategyFactory {
        storage_config: first_storage,
        env_config,
        logical_topic_name,
        max_batch_size,
        max_batch_time,
        processing_concurrency: ConcurrencyConfig::new(concurrency),
        clickhouse_concurrency: ConcurrencyConfig::new(clickhouse_concurrency),
        commitlog_concurrency: ConcurrencyConfig::new(2),
        replacements_concurrency: ConcurrencyConfig::new(4),
        async_inserts,
        python_max_queue_depth,
        use_rust_processor,
        health_check_file: health_check_file.map(ToOwned::to_owned),
        enforce_schema,
        commit_log_producer,
        replacements_config,
        physical_consumer_group: consumer_group.to_owned(),
        physical_topic_name: Topic::new(&consumer_config.raw_topic.physical_topic_name),
        accountant_topic_config: consumer_config.accountant_topic,
        stop_at_timestamp,
        batch_write_timeout,
        custom_envoy_request_timeout,
        join_timeout_ms,
        health_check: health_check.to_string(),
    };

    let processor = StreamProcessor::with_kafka(config, factory, topic, dlq_policy);

    let mut handle = processor.get_handle();

    match rebalance_delay_secs {
        Some(secs) => {
            ctrlc::set_handler(move || {
                rebalancing::delay_kafka_rebalance(secs);
                handle.signal_shutdown();
            })
            .expect("Error setting Ctrl-C handler");
        }
        None => {
            ctrlc::set_handler(move || {
                handle.signal_shutdown();
            })
            .expect("Error setting Ctrl-C handler");
        }
    }

    if let Err(error) = processor.run() {
        let error: &dyn std::error::Error = &error;
        tracing::error!("{:?}", error);
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
    let func = processors::get_processing_function(name).ok_or(SnubaRustError::new_err(
        format!("processor '{}' not found", name),
    ))?;

    let payload = KafkaPayload::new(None, None, Some(value));
    let timestamp = DateTime::<Utc>::from_timestamp_millis(millis_since_epoch).unwrap_or_default();
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
