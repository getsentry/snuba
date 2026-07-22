use sentry::integrations::tracing::EventFilter;
use sentry::ClientInitGuard;
use tracing::Level;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

pub fn setup_logging() {
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    // Only errors are forwarded to Sentry as issues. Warnings are operational
    // noise far more often than they're actionable (e.g. consumer
    // rebalance/shutdown timeouts), so they're kept as logs alongside
    // everything at or above INFO, instead of breadcrumbs.
    let sentry_layer = sentry::integrations::tracing::layer().event_filter(|metadata| {
        match *metadata.level() {
            // The usage accountant is a best-effort billing side channel. Its
            // Kafka producer logs "Purged in queue/flight" at ERROR whenever the
            // producer is flushed during a consumer shutdown/rebalance. These
            // are transient and don't affect ingestion, so keep them as logs
            // rather than issues (SNUBA-474, SNUBA-475).
            Level::ERROR if metadata.target().starts_with("sentry_usage_accountant") => {
                EventFilter::Log
            }
            // The DogStatsD forwarder logs at ERROR on every undeliverable flush
            // (e.g. "Connection refused"), which self-heals but floods Sentry when
            // the target is unreachable. Keep it as logs only (SNUBA-BQ8).
            Level::ERROR if metadata.target().starts_with("metrics_exporter_dogstatsd") => {
                EventFilter::Log
            }
            Level::ERROR => EventFilter::Event | EventFilter::Log,
            Level::WARN | Level::INFO => EventFilter::Log,
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
