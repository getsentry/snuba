use std::time::Duration;

use pyo3::prelude::*;
use sentry_arroyo::metrics;
use sentry_arroyo::processing::stream::{PipelineRunner, PullSource};

use crate::config;
use crate::logging::{setup_logging, setup_sentry};
use crate::metrics::statsd::create_dogstatsd_backend;

use super::factories;
use super::pipelines;
use super::{EAP_PROCESSORS, FIRE_AND_FORGET_PROCESSORS};

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn pull_consumer(
    py: Python<'_>,
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
    max_poll_interval_ms: usize,
    dry_run_latency_ms: u64,
    use_row_binary: bool,
) -> usize {
    py.allow_threads(|| {
        pull_consumer_impl(
            consumer_group,
            auto_offset_reset,
            no_strict_offset_reset,
            consumer_config_raw,
            processing_concurrency,
            clickhouse_concurrency,
            max_poll_interval_ms,
            dry_run_latency_ms,
            use_row_binary,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn pull_consumer_impl(
    consumer_group: &str,
    auto_offset_reset: &str,
    no_strict_offset_reset: bool,
    consumer_config_raw: &str,
    processing_concurrency: usize,
    clickhouse_concurrency: usize,
    max_poll_interval_ms: usize,
    dry_run_latency_ms: u64,
    use_row_binary: bool,
) -> usize {
    setup_logging();
    crate::init_sentry_options().expect("failed to initialize sentry-options");

    let consumer_config = config::ConsumerConfig::load_from_str(consumer_config_raw)
        .expect("failed to parse consumer config");

    assert_eq!(
        consumer_config.storages.len(),
        1,
        "pull consumer only supports a single storage"
    );

    let storage = consumer_config.storages[0].clone();
    let processor_name = storage.message_processor.python_class_name.clone();
    let env_config = consumer_config.env.clone();

    // Sentry
    let mut _sentry_guard = None;
    if let Some(ref dsn) = consumer_config.env.sentry_dsn {
        std::env::set_var("RUST_BACKTRACE", "1");
        _sentry_guard = Some(setup_sentry(dsn));
    }

    // Metrics
    {
        let tags = [
            ("storage", storage.name.clone()),
            ("consumer_group", consumer_group.to_owned()),
        ];
        sentry::configure_scope(|scope| {
            scope.set_tag("storage", &storage.name);
            scope.set_tag("consumer_group", consumer_group);
        });
        if let Some(backend) = create_dogstatsd_backend(&env_config, "snuba.consumer", &tags) {
            metrics::init(backend).unwrap();
        }
    }

    let processor = match factories::resolve_processor(&processor_name) {
        Ok(f) => f,
        Err(msg) => {
            tracing::error!("{msg}");
            return 1;
        }
    };

    // Validate pipeline type
    let is_eap = EAP_PROCESSORS.contains(&processor_name.as_str());
    let is_faf = FIRE_AND_FORGET_PROCESSORS.contains(&processor_name.as_str());
    if !is_eap && !is_faf {
        tracing::error!("{processor_name} is not supported by the pull consumer");
        return 1;
    }

    let dry_run = dry_run_latency_ms > 0;
    let dry_run_latency = if dry_run {
        Some(Duration::from_millis(dry_run_latency_ms))
    } else {
        None
    };

    tracing::info!(
        storage = storage.name,
        processor = processor_name.as_str(),
        pipeline = if is_eap { "eap" } else { "fire_and_forget" },
        dry_run,
        dry_run_latency_ms,
        use_row_binary,
        "Starting pull consumer",
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let exit_code = rt.block_on(async {
        let source = factories::make_kafka_source(
            &consumer_config,
            consumer_group,
            auto_offset_reset,
            no_strict_offset_reset,
            max_poll_interval_ms,
        );
        let processor_config = factories::make_processor_config(&storage, &env_config);

        let shared = factories::make_shared_resources(
            &consumer_config,
            &storage,
            &processor_config,
            use_row_binary,
            dry_run,
            dry_run_latency,
        );

        let result = if is_eap {
            PipelineRunner::run(&source, Duration::from_secs(1), || {
                pipelines::make_eap_pipeline(
                    &shared,
                    &consumer_config,
                    &storage,
                    &processor_config,
                    consumer_group,
                    processing_concurrency,
                    clickhouse_concurrency,
                    dry_run,
                )
            })
            .await
        } else {
            PipelineRunner::run(&source, Duration::from_secs(1), || {
                pipelines::make_faf_pipeline(
                    &shared,
                    processor,
                    &consumer_config,
                    &processor_config,
                    processing_concurrency,
                    clickhouse_concurrency,
                )
            })
            .await
        };

        source.shutdown();

        match result {
            Ok(()) => 0,
            Err(e) => {
                tracing::error!("Pipeline failed: {e}");
                1
            }
        }
    });

    exit_code
}
