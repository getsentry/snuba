use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use sentry_arroyo::metrics::{Metric, MetricType, MetricValue, Recorder};

use crate::config::EnvConfig;
use crate::metrics::global_tags::get_global_tags;

/// A metrics backend that uses `metrics-exporter-dogstatsd` to send metrics
/// to DogStatsD over UDP or Unix domain sockets. Adapts arroyo's [`Recorder`]
/// trait to the `metrics` crate facade installed by the exporter.
#[derive(Debug)]
pub struct DogStatsDBackend;

impl DogStatsDBackend {
    pub fn new_udp(host: &str, port: u16, prefix: &str, tags: &[(&str, String)]) -> Self {
        let addr = format!("{host}:{port}");
        Self::build(&addr, prefix, tags)
    }

    /// `socket_path` is passed to the exporter verbatim, so it must be a full DogStatsD
    /// remote address including the transport scheme, e.g. `unixgram:///run/dogstatsd.sock`
    /// (datagram) or `unix:///run/dogstatsd.sock` (stream). The scheme is supplied via the
    /// `SNUBA_DOGSTATSD_SOCKET_PATH` env var rather than hardcoded here, so the same value
    /// also works for the Python datadog client (which strips the scheme itself).
    pub fn new_uds(socket_path: &str, prefix: &str, tags: &[(&str, String)]) -> Self {
        Self::build(socket_path, prefix, tags)
    }

    fn build(addr: &str, prefix: &str, tags: &[(&str, String)]) -> Self {
        let global_labels: Vec<Label> = tags
            .iter()
            .map(|(k, v)| Label::new(k.to_string(), v.clone()))
            .collect();

        DogStatsDBuilder::default()
            .with_remote_address(addr)
            .expect("invalid DogStatsD address")
            .set_global_prefix(prefix)
            .with_global_labels(global_labels)
            .send_histograms_as_distributions(false)
            .with_telemetry(true)
            .install()
            .expect("failed to install DogStatsD exporter");

        Self
    }
}

/// The DogStatsD transport selected for the current runtime configuration.
#[derive(Debug, PartialEq, Eq)]
enum DogStatsDTransport<'a> {
    /// Unix domain socket at the given path.
    Uds(&'a str),
    /// UDP to the given host and port.
    Udp(&'a str, u16),
    /// No transport configured; metrics are disabled.
    Disabled,
}

/// Decide which DogStatsD transport to use.
///
/// The `use_uds` flag selects the *preferred* transport: when it is set and a socket path
/// is configured, metrics go over UDS; otherwise they go over UDP (host/port). If only one
/// transport is configured it is used regardless of the flag — a socket-only deployment
/// sends over UDS, and a host/port-only deployment sends over UDP. The flag is therefore
/// authoritative only when both are configured (a configured socket never overrides an
/// available UDP target while the flag is off), which keeps host/port as the UDP rollback
/// target. With neither configured, metrics are disabled. Matches the Python
/// `create_metrics()` gating. Kept pure (no global recorder install) so it is unit-testable.
fn select_transport(env: &EnvConfig, use_uds: bool) -> DogStatsDTransport<'_> {
    let socket = env.dogstatsd_socket_path.as_deref();
    let udp = match (env.dogstatsd_host.as_deref(), env.dogstatsd_port) {
        (Some(host), Some(port)) => Some((host, port)),
        _ => None,
    };
    match socket {
        // UDS when a socket is configured and either the flag selects it or there is no
        // UDP (host/port) target to fall back to.
        Some(socket_path) if use_uds || udp.is_none() => DogStatsDTransport::Uds(socket_path),
        // Otherwise use UDP when host/port are configured; with neither, metrics are off.
        _ => match udp {
            Some((host, port)) => DogStatsDTransport::Udp(host, port),
            None => DogStatsDTransport::Disabled,
        },
    }
}

/// Read the `use_dogstatsd_uds` rollout flag from the `snuba` sentry-options namespace.
///
/// Reads the same store as the rest of snuba's Rust sentry-options access (see
/// `snuba_options.rs`) and as the Python `create_metrics()`
/// (`snuba.state.sentry_options.get_option`). The option is declared as a boolean in
/// `sentry-options/schemas/snuba/schema.json`. Any failure (options client
/// uninitialized, key missing, wrong type) defaults to `false`, i.e. the stable UDP path.
/// `sentry_options::init_with_schemas` is called at consumer startup before the metrics
/// backend is built, so the option is available by the time this runs.
fn use_dogstatsd_uds_enabled() -> bool {
    sentry_options::options("snuba")
        .ok()
        .and_then(|o| o.get("use_dogstatsd_uds").ok())
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Build the DogStatsD metrics backend, choosing the transport at runtime.
///
/// The `use_dogstatsd_uds` sentry-option selects the preferred transport: with it `true`
/// and a socket path configured, metrics go over UDS; otherwise UDP (host/port). A
/// deployment configured with only one of the two uses that one regardless of the flag.
/// This mirrors the gating in the Python `create_metrics()` so the transport can be
/// switched by flipping the sentry-option (followed by a restart) without a redeploy. If
/// the flag cannot be read (e.g. options unavailable) we default to `false` (prefer UDP).
///
/// Returns `None` when neither transport is configured, leaving metrics disabled.
pub fn create_dogstatsd_backend(
    env: &EnvConfig,
    prefix: &str,
    tags: &[(&str, String)],
) -> Option<DogStatsDBackend> {
    let use_uds = use_dogstatsd_uds_enabled();

    // Tag every metric with the transport protocol in use, so metrics can be sliced by
    // UDS vs UDP during (and after) the socket migration.
    let mut tags = tags.to_vec();
    match select_transport(env, use_uds) {
        DogStatsDTransport::Uds(socket_path) => {
            tags.push(("dogstatsd_transport", "uds".to_owned()));
            Some(DogStatsDBackend::new_uds(socket_path, prefix, &tags))
        }
        DogStatsDTransport::Udp(host, port) => {
            tags.push(("dogstatsd_transport", "udp".to_owned()));
            Some(DogStatsDBackend::new_udp(host, port, prefix, &tags))
        }
        DogStatsDTransport::Disabled => None,
    }
}

impl Recorder for DogStatsDBackend {
    fn record_metric(&self, metric: Metric<'_>) {
        let key: metrics::SharedString = metric.key.to_string().into();
        let mut labels: Vec<Label> = metric
            .tags
            .iter()
            .map(|(k, v)| Label::new(k.to_string(), v.to_string()))
            .collect();

        for (k, v) in get_global_tags() {
            labels.push(Label::new(k, v));
        }
        let metadata = metrics::Metadata::new("snuba", metrics::Level::INFO, None);
        let key = metrics::Key::from_parts(key, labels);

        match metric.ty {
            MetricType::Counter => {
                let value = match metric.value {
                    MetricValue::I64(v) => v as u64,
                    MetricValue::U64(v) => v,
                    MetricValue::F64(v) => v as u64,
                    MetricValue::Duration(d) => d.as_millis() as u64,
                    _ => return,
                };
                metrics::with_recorder(|rec| {
                    rec.register_counter(&key, &metadata).increment(value);
                });
            }
            MetricType::Gauge => {
                let value = match metric.value {
                    MetricValue::I64(v) => v as f64,
                    MetricValue::U64(v) => v as f64,
                    MetricValue::F64(v) => v,
                    MetricValue::Duration(d) => d.as_millis() as f64,
                    _ => return,
                };
                metrics::with_recorder(|rec| {
                    rec.register_gauge(&key, &metadata).set(value);
                });
            }
            MetricType::Timer => {
                let value = match metric.value {
                    MetricValue::I64(v) => v as f64,
                    MetricValue::U64(v) => v as f64,
                    MetricValue::F64(v) => v,
                    MetricValue::Duration(d) => d.as_millis() as f64,
                    _ => return,
                };
                metrics::with_recorder(|rec| {
                    rec.register_histogram(&key, &metadata).record(value);
                });
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use sentry_arroyo::metric;

    use super::*;

    #[test]
    fn dogstatsd_metric_backend() {
        let backend = DogStatsDBackend::new_udp("0.0.0.0", 8125, "test", &[]);

        backend.record_metric(metric!(Counter: "a", 1, "tag1" => "value1"));
        backend.record_metric(metric!(Gauge: "b", 20, "tag2" => "value2"));
        backend.record_metric(metric!(Timer: "c", 30, "tag3" => "value3"));
    }

    fn env_with(host: Option<&str>, port: Option<u16>, socket: Option<&str>) -> EnvConfig {
        EnvConfig {
            dogstatsd_host: host.map(str::to_string),
            dogstatsd_port: port,
            dogstatsd_socket_path: socket.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn uds_only_when_flag_on_and_socket_set() {
        let env = env_with(
            Some("localhost"),
            Some(8125),
            Some("unixgram:///var/run/dd.sock"),
        );
        // Flag on + socket present -> UDS.
        assert_eq!(
            select_transport(&env, true),
            DogStatsDTransport::Uds("unixgram:///var/run/dd.sock")
        );
        // Flag off + socket still present -> stays on UDP. This is the key property:
        // a deployed socket path does not force UDS, so flipping the flag back to "0"
        // rolls back to UDP without a redeploy.
        assert_eq!(
            select_transport(&env, false),
            DogStatsDTransport::Udp("localhost", 8125)
        );
    }

    #[test]
    fn falls_back_to_udp_without_socket() {
        let env = env_with(Some("localhost"), Some(8125), None);
        // Even with the flag on, no socket path means UDP.
        assert_eq!(
            select_transport(&env, true),
            DogStatsDTransport::Udp("localhost", 8125)
        );
        assert_eq!(
            select_transport(&env, false),
            DogStatsDTransport::Udp("localhost", 8125)
        );
    }

    #[test]
    fn disabled_when_nothing_configured() {
        // Neither UDP (host/port) nor UDS (socket) configured -> metrics disabled.
        let env = env_with(None, None, None);
        assert_eq!(select_transport(&env, true), DogStatsDTransport::Disabled);
        assert_eq!(select_transport(&env, false), DogStatsDTransport::Disabled);
    }

    #[test]
    fn uds_when_socket_only() {
        // A socket alone (no host/port) is a usable transport: metrics go over UDS
        // regardless of the flag, since there is no UDP target to prefer or fall back to.
        let env = env_with(None, None, Some("unixgram:///var/run/dd.sock"));
        assert_eq!(
            select_transport(&env, true),
            DogStatsDTransport::Uds("unixgram:///var/run/dd.sock")
        );
        assert_eq!(
            select_transport(&env, false),
            DogStatsDTransport::Uds("unixgram:///var/run/dd.sock")
        );
    }
}
