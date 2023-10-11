use sentry::ClientInitGuard;

pub fn setup_logging() {
    let mut log_builder = pretty_env_logger::formatted_builder();
    log_builder.parse_env("RUST_LOG");
    let logger = sentry::integrations::log::SentryLogger::with_dest(log_builder.build());

    log::set_boxed_logger(Box::new(logger)).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
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
