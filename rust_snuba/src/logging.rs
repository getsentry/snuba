use std::sync::Arc;

use sentry::integrations::tracing::EventFilter;
use sentry::protocol::{Event, Log};
use sentry::ClientInitGuard;
use sentry_arroyo::counter;
use tracing::Level;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

const ARROYO_KAFKA_LOGGER: &str = "sentry_arroyo::backends::kafka";
const BROKER_TRANSPORT_FAILURE: &str =
    "Global error: BrokerTransportFailure (Local: Broker transport failure)";
const BROKER_TRANSPORT_FAILURE_LOG: &str =
    "librdkafka: Global error: BrokerTransportFailure (Local: Broker transport failure)";

fn is_broker_transport_failure_event(event: &Event<'_>) -> bool {
    event.logger.as_deref() == Some(ARROYO_KAFKA_LOGGER)
        && event.exception.values.iter().any(|exception| {
            exception.ty == "KafkaError"
                && exception
                    .value
                    .as_deref()
                    .is_some_and(|value| value.starts_with(BROKER_TRANSPORT_FAILURE))
        })
}

fn is_broker_transport_failure_log(log: &Log) -> bool {
    log.body.starts_with(BROKER_TRANSPORT_FAILURE_LOG)
}

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
            before_send: Some(Arc::new(|event| {
                if is_broker_transport_failure_event(&event) {
                    counter!(
                        "rust_consumer.kafka_error",
                        1,
                        "error" => "broker_transport_failure"
                    );
                    None
                } else {
                    Some(event)
                }
            })),
            // The tracing integration sends errors as both events and structured
            // logs. The event callback above records the metric; drop the duplicate
            // log as well so this recoverable broker outage does not reach Sentry.
            before_send_log: Some(Arc::new(|log| {
                (!is_broker_transport_failure_log(&log)).then_some(log)
            })),
            ..Default::default()
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use sentry::protocol::{Exception, LogLevel};

    use super::*;

    fn event(logger: &str, exception_type: &str, value: &str) -> Event<'static> {
        Event {
            logger: Some(logger.to_owned()),
            exception: vec![Exception {
                ty: exception_type.to_owned(),
                value: Some(value.to_owned()),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        }
    }

    fn log(body: &str) -> Log {
        Log {
            level: LogLevel::Error,
            body: body.to_owned(),
            trace_id: None,
            timestamp: SystemTime::now(),
            severity_number: None,
            attributes: Default::default(),
        }
    }

    #[test]
    fn identifies_global_broker_transport_failure_event() {
        assert!(is_broker_transport_failure_event(&event(
            ARROYO_KAFKA_LOGGER,
            "KafkaError",
            "Global error: BrokerTransportFailure (Local: Broker transport failure)",
        )));
    }

    #[test]
    fn preserves_other_kafka_events() {
        assert!(!is_broker_transport_failure_event(&event(
            ARROYO_KAFKA_LOGGER,
            "KafkaError",
            "Global error: Authentication (Local: Authentication failure)",
        )));
        assert!(!is_broker_transport_failure_event(&event(
            "snuba::consumer",
            "KafkaError",
            "Global error: BrokerTransportFailure (Local: Broker transport failure)",
        )));
    }

    #[test]
    fn identifies_only_broker_transport_failure_log() {
        assert!(is_broker_transport_failure_log(&log(
            "librdkafka: Global error: BrokerTransportFailure (Local: Broker transport failure): broker:9092 disconnected",
        )));
        assert!(!is_broker_transport_failure_log(&log(
            "librdkafka: Global error: Authentication (Local: Authentication failure)",
        )));
    }
}
