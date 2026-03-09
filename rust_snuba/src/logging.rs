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
    let sentry_layer =
        sentry::integrations::tracing::layer().event_filter(|metadata| match *metadata.level() {
            Level::ERROR | Level::WARN => EventFilter::Event | EventFilter::Log,
            Level::INFO => EventFilter::Log,
            Level::DEBUG | Level::TRACE => EventFilter::Ignore,
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
