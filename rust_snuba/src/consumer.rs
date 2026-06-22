use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_arroyo::metrics;
use sentry_arroyo::processing::dlq::{DlqLimit, DlqPolicy, KafkaDlqProducer};

use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::{ProcessorHandle, StreamProcessor};
use sentry_arroyo::types::Topic;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use sentry_options::init_with_schemas;

use crate::auto_restart;
use crate::config;
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
    join_timeout_ms: Option<u64>,
    use_row_binary: bool,
) -> usize {
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
            join_timeout_ms,
            health_check,
            use_row_binary,
        )
    })
}

/// Outcome of a single consumer run inside the restart loop, used to resolve
/// the race between the periodic-restart timer firing and the run ending for
/// another reason (e.g. `stop_at_timestamp`). Whichever side records its
/// outcome first via compare-and-swap wins.
const OUTCOME_UNDECIDED: u8 = 0;
const OUTCOME_RESTART: u8 = 1;
const OUTCOME_STOP: u8 = 2;

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
    join_timeout_ms: Option<u64>,
    health_check: &str,
    use_row_binary: bool,
) -> usize {
    setup_logging();
    init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)])
        .expect("failed to initialize sentry-options");

    // Parse the config once up front for process-wide, one-time setup (logging,
    // Sentry, metrics, etc.). The consumer itself is (re)built from a fresh
    // parse inside the restart loop below, so a periodic restart can reconnect
    // to Kafka without tearing the whole process down.
    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();

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

    // The "quantized rebalance" delay may be set statically in the consumer
    // config and/or overridden live via runtime config. We capture the static
    // value once (it comes from the immutable startup config) and refresh the
    // runtime override on every restart in the loop below, so live updates take
    // effect without a full process restart.
    let static_rebalance_delay_secs = consumer_config
        .raw_topic
        .quantized_rebalance_consumer_group_delay_secs;

    // `shutdown_requested` is set by the Ctrl-C handler to permanently stop the
    // process. Whether a given run should restart (vs exit) is tracked per
    // iteration via an `OUTCOME_*` atomic in the loop below.
    let shutdown_requested = Arc::new(AtomicBool::new(false));

    // The Ctrl-C handler can only be installed once per process, but the
    // consumer (and therefore its `ProcessorHandle`) is rebuilt on every
    // restart. We share the live handle through a mutex so the handler always
    // signals the consumer that is currently running. The current rebalance
    // delay is shared the same way so the handler quantizes its "leave" using
    // the latest configured value.
    let current_handle: Arc<Mutex<Option<ProcessorHandle>>> = Arc::new(Mutex::new(None));
    let rebalance_delay_secs = Arc::new(Mutex::new(
        rebalancing::get_rebalance_delay_secs(consumer_group).or(static_rebalance_delay_secs),
    ));

    {
        let shutdown_requested = shutdown_requested.clone();
        let current_handle = current_handle.clone();
        let rebalance_delay_secs = rebalance_delay_secs.clone();
        ctrlc::set_handler(move || {
            shutdown_requested.store(true, Ordering::SeqCst);
            // Copy the delay out and release the lock before the (potentially
            // multi-minute) sleep, so the main loop never blocks waiting for
            // this handler to release the lock.
            let delay = *rebalance_delay_secs.lock().unwrap();
            if let Some(secs) = delay {
                rebalancing::delay_kafka_rebalance(secs);
            }
            if let Some(handle) = current_handle.lock().unwrap().as_mut() {
                handle.signal_shutdown();
            }
        })
        .expect("Error setting Ctrl-C handler");
    }

    // Restart loop. Each iteration builds a fresh consumer (reconnecting to
    // Kafka) and runs it until it shuts down. A shutdown caused by the periodic
    // restart timer loops back around to rebuild the consumer; any other
    // graceful shutdown (Ctrl-C, `stop_at_timestamp`, ...) exits the loop.
    let mut first_run = true;
    loop {
        // Refresh the rebalance delay from runtime config so live updates take
        // effect on each restart, and publish it for the Ctrl-C handler.
        let current_rebalance_delay_secs =
            rebalancing::get_rebalance_delay_secs(consumer_group).or(static_rebalance_delay_secs);
        *rebalance_delay_secs.lock().unwrap() = current_rebalance_delay_secs;

        let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw).unwrap();
        let max_batch_size = consumer_config.max_batch_size;
        let max_batch_time = Duration::from_millis(consumer_config.max_batch_time_ms);
        let max_batch_size_calculation = consumer_config.max_batch_size_calculation;

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

        let env_config = consumer_config.env.clone();
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

        let blq_producer_config = consumer_config.dlq_topic.as_ref().map(|dlq_topic_config| {
            let mut overrides = dlq_topic_config.broker_config.clone();
            overrides.insert("message.max.bytes".to_string(), "10000000".to_string()); // 10 MB, broker max
            KafkaConfig::new_producer_config(vec![], Some(overrides))
        });

        let dlq_topic = consumer_config
            .dlq_topic
            .as_ref()
            .map(|dlq_topic_config| Topic::new(&dlq_topic_config.physical_topic_name));

        let dlq_policy = consumer_config.dlq_topic.map(|dlq_topic_config| {
            let producer = KafkaProducer::new(KafkaConfig::new_producer_config(
                vec![],
                Some(dlq_topic_config.broker_config),
            ));

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

        let factory = ConsumerStrategyFactoryV2 {
            storage_config: first_storage,
            env_config,
            logical_topic_name,
            max_batch_size,
            max_batch_time,
            max_batch_size_calculation,
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
            join_timeout_ms,
            health_check: health_check.to_string(),
            use_row_binary,
            blq_producer_config: blq_producer_config.clone(),
            blq_topic: dlq_topic,
        };

        let processor = StreamProcessor::with_kafka(config, factory, topic, dlq_policy);

        // Publish this consumer's handle so the Ctrl-C handler signals the live
        // consumer.
        let handle = processor.get_handle();
        *current_handle.lock().unwrap() = Some(handle.clone());

        // If a shutdown was requested before we managed to (re)build the
        // consumer, stop now instead of starting it.
        if shutdown_requested.load(Ordering::SeqCst) {
            break;
        }

        // Quantize the initial join when configured, matching the original
        // startup behaviour. We only do this on the first run: on a periodic
        // restart the timer below already quantized the corresponding "leave",
        // so delaying the rejoin too would keep the consumer offline for two
        // delay periods instead of one.
        if first_run {
            if let Some(secs) = current_rebalance_delay_secs {
                rebalancing::delay_kafka_rebalance(secs);
            }
            // A shutdown may have arrived during the delay; stop now instead of
            // starting the consumer and spawning a needless restart timer.
            if shutdown_requested.load(Ordering::SeqCst) {
                break;
            }
        }
        first_run = false;

        // Tracks whether this run ends in a restart or a stop. The timer and the
        // main thread both record their outcome via compare-and-swap, so a timer
        // firing concurrently with a non-restart shutdown can't turn it into a
        // restart (and vice versa) — whichever records first wins.
        let restart_outcome = Arc::new(AtomicU8::new(OUTCOME_UNDECIDED));

        // Periodically restart the consumer to reconnect to Kafka when the
        // periodic-restart feature flag is enabled for this consumer group.
        // After the configured interval (default 15 minutes) we signal a
        // graceful shutdown; the restart loop then rebuilds the consumer, which
        // re-establishes its Kafka connection. See `crate::auto_restart` for the
        // controlling runtime config keys.
        //
        // The timer waits on a channel rather than a bare sleep so that, when
        // the run ends for any reason, we can stop and join it below instead of
        // leaking a sleeping thread.
        let restart_timer = auto_restart::get_restart_interval(consumer_group).map(|interval| {
            tracing::info!(
                consumer_group,
                interval_secs = interval.as_secs(),
                "Periodic Kafka reconnect enabled; consumer will restart after the configured interval",
            );
            let (stop_tx, stop_rx) = mpsc::channel::<()>();
            let mut restart_handle = handle.clone();
            let restart_outcome = restart_outcome.clone();
            let consumer_group = consumer_group.to_owned();
            let join = thread::spawn(move || {
                // Wake up either when the interval elapses or when we are asked
                // to stop (the run ended for another reason).
                if stop_rx.recv_timeout(interval) != Err(mpsc::RecvTimeoutError::Timeout) {
                    return;
                }
                // Honor the same rebalance quantization the Ctrl-C handler uses
                // so a periodic restart does not trigger extra Kafka rebalances
                // for groups that also enable quantized rebalancing. We wait on
                // the stop channel rather than sleeping, so that if the run ends
                // for another reason during this delay we can bail immediately
                // instead of blocking the join below.
                if let Some(secs) = current_rebalance_delay_secs {
                    let delay = rebalancing::quantized_rebalance_delay(secs);
                    if stop_rx.recv_timeout(delay) != Err(mpsc::RecvTimeoutError::Timeout) {
                        return;
                    }
                }
                // Only signal a restart if the main thread hasn't already
                // decided this run is stopping for another reason.
                if restart_outcome
                    .compare_exchange(
                        OUTCOME_UNDECIDED,
                        OUTCOME_RESTART,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    tracing::info!(
                        consumer_group,
                        interval_secs = interval.as_secs(),
                        "Periodic restart interval elapsed; signaling shutdown to reconnect to Kafka",
                    );
                    counter!("periodic_restart");
                    restart_handle.signal_shutdown();
                }
            });
            (stop_tx, join)
        });

        let result = processor.run();

        // Record that this run is stopping unless the timer already claimed a
        // restart. Doing this before stopping the timer ensures a timer that
        // fires concurrently with a non-restart shutdown won't trigger a
        // restart.
        let _ = restart_outcome.compare_exchange(
            OUTCOME_UNDECIDED,
            OUTCOME_STOP,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );

        // The run has ended: stop the restart timer (if it hasn't already fired)
        // and join it so we never leave an orphaned sleeping thread behind.
        if let Some((stop_tx, join)) = restart_timer {
            drop(stop_tx);
            let _ = join.join();
        }

        match result {
            Ok(()) => {
                if shutdown_requested.load(Ordering::SeqCst) {
                    break;
                }
                if restart_outcome.load(Ordering::SeqCst) == OUTCOME_RESTART {
                    counter!("consumer_reconnect");
                    tracing::info!("Restarting consumer to reconnect to Kafka");
                    continue;
                }
                // Graceful termination that was not a periodic restart (e.g.
                // `stop_at_timestamp`): exit the loop.
                break;
            }
            Err(error) => {
                let error: &dyn std::error::Error = &error;
                tracing::error!("{:?}", error);
                return 1;
            }
        }
    }

    0
}

mod exceptions {
    pyo3::create_exception!(rust_snuba, SnubaRustError, pyo3::exceptions::PyException);
}
use exceptions::*;

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
        format!("processor '{name}' not found"),
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
                .map_err(|e| SnubaRustError::new_err(format!("invalid message: {e:?}")))?;

            let payload = PyBytes::new(py, &res.rows.into_encoded_rows()).into();

            Ok((Some(payload), None))
        }
        processors::ProcessingFunctionType::ProcessingFunctionWithReplacements(f) => {
            let res = f(payload, meta, &config::ProcessorConfig::default())
                .map_err(|e| SnubaRustError::new_err(format!("invalid message: {e:?}")))?;

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
