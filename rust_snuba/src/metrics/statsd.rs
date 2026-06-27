use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use sentry_arroyo::metrics::{Metric, MetricType, MetricValue, Recorder};

use crate::config::EnvConfig;
use crate::metrics::global_tags::get_global_tags;
use crate::runtime_config::get_str_config;

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

    pub fn new_uds(socket_path: &str, prefix: &str, tags: &[(&str, String)]) -> Self {
        let addr = format!("unixgram://{socket_path}");
        Self::build(&addr, prefix, tags)
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
/// UDS is selected only when `use_uds` is true *and* a socket path is configured;
/// otherwise we fall back to UDP (host/port), and to disabled when nothing is
/// configured. Crucially, a configured socket path alone does *not* force UDS — the
/// runtime flag gates it — so a deployment can ship with both transports available
/// and flip between them at runtime. Kept pure (no global recorder install) so the
/// gating is unit-testable.
fn select_transport(env: &EnvConfig, use_uds: bool) -> DogStatsDTransport<'_> {
    match (use_uds, env.dogstatsd_socket_path.as_deref()) {
        (true, Some(socket_path)) => DogStatsDTransport::Uds(socket_path),
        _ => match (env.dogstatsd_host.as_deref(), env.dogstatsd_port) {
            (Some(host), Some(port)) => DogStatsDTransport::Udp(host, port),
            _ => DogStatsDTransport::Disabled,
        },
    }
}

/// Build the DogStatsD metrics backend, choosing the transport at runtime.
///
/// UDS is used only when the `use_dogstatsd_uds` runtime flag is set to `"1"` *and*
/// a socket path is configured; otherwise we fall back to UDP (host/port). This
/// mirrors the gating in the Python `create_metrics()` so the transport can be
/// switched by flipping the Redis flag (followed by a restart) without a redeploy.
/// If the flag cannot be read (e.g. runtime config unavailable), we default to the
/// stable UDP path.
///
/// Returns `None` when neither transport is configured, leaving metrics disabled.
pub fn create_dogstatsd_backend(
    env: &EnvConfig,
    prefix: &str,
    tags: &[(&str, String)],
) -> Option<DogStatsDBackend> {
    let use_uds = matches!(
        get_str_config("use_dogstatsd_uds")
            .ok()
            .flatten()
            .as_deref(),
        Some("1")
    );

    match select_transport(env, use_uds) {
        DogStatsDTransport::Uds(socket_path) => {
            Some(DogStatsDBackend::new_uds(socket_path, prefix, tags))
        }
        DogStatsDTransport::Udp(host, port) => {
            Some(DogStatsDBackend::new_udp(host, port, prefix, tags))
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
        let env = env_with(Some("localhost"), Some(8125), Some("/var/run/dd.sock"));
        // Flag on + socket present -> UDS.
        assert_eq!(
            select_transport(&env, true),
            DogStatsDTransport::Uds("/var/run/dd.sock")
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
    fn disabled_when_no_transport_available() {
        // Nothing configured at all.
        let env = env_with(None, None, None);
        assert_eq!(select_transport(&env, true), DogStatsDTransport::Disabled);
        assert_eq!(select_transport(&env, false), DogStatsDTransport::Disabled);
        // Socket set but flag off and no host/port to fall back to -> disabled.
        let env = env_with(None, None, Some("/var/run/dd.sock"));
        assert_eq!(select_transport(&env, false), DogStatsDTransport::Disabled);
    }
}
