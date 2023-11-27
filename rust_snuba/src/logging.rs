use sentry::integrations::tracing::EventFilter;
use sentry::ClientInitGuard;
use tracing::Level;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

pub fn setup_logging() {
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    // Capture errors & warnings as exceptions
    let sentry_layer =
        sentry::integrations::tracing::layer().event_filter(|metadata| match metadata.level() {
            &Level::ERROR | &Level::WARN => EventFilter::Exception,
            &Level::INFO => EventFilter::Breadcrumb,
            &Level::DEBUG | &Level::TRACE => EventFilter::Ignore,
        });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter_layer)
        .with(sentry_layer)
        .init();
}

pub fn setup_sentry(sentry_dsn: String) -> ClientInitGuard {
    sentry::init((
        sentry_dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ))
}
