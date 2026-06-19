use sentry::integrations::tracing::EventFilter;
use sentry::ClientInitGuard;
use tracing::Level;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

pub fn setup_logging() {
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    // Capture errors & warnings as exceptions, and also send everything at or above INFO as logs
    // instead of breadcrumbs.
    //
    // A few targets emit high-volume transient operational noise that we keep as
    // logs but don't want to surface as Sentry issues. Match on the tracing
    // target (the emitting module path) so these stay observable without
    // creating ongoing issues.
    let sentry_layer = sentry::integrations::tracing::layer().event_filter(|metadata| {
        let target = metadata.target();
        match *metadata.level() {
            // The usage accountant is a best-effort billing side channel. Its
            // Kafka producer logs "Purged in queue/flight" errors whenever the
            // producer is flushed during a consumer shutdown/rebalance. These
            // are transient and don't affect ingestion (SNUBA-474, SNUBA-475).
            Level::ERROR | Level::WARN if target.starts_with("sentry_usage_accountant") => {
                EventFilter::Log
            }
            // Arroyo logs task-join timeouts at WARN while draining in-flight
            // work during shutdown. Expected on every deploy/rebalance, so
            // they're not actionable (SNUBA-4VF, SNUBA-4WS).
            Level::WARN
                if target == "sentry_arroyo::processing::strategies::run_task_in_threads"
                    || target == "sentry_arroyo::processing::strategies::reduce" =>
            {
                EventFilter::Log
            }
            Level::ERROR | Level::WARN => EventFilter::Event | EventFilter::Log,
            Level::INFO => EventFilter::Log,
            Level::DEBUG | Level::TRACE => EventFilter::Ignore,
        }
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().json())
        .with(filter_layer)
        .with(sentry_layer)
        .init();
}

pub fn setup_sentry(sentry_dsn: &str) -> ClientInitGuard {
    sentry::init((
        sentry_dsn,
        sentry::ClientOptions {
            // the value for release is also computed in python snuba, please keep the
            // logic in sync
            release: std::env::var("SNUBA_RELEASE").ok().map(From::from),
            enable_logs: true,
            ..Default::default()
        },
    ))
}
