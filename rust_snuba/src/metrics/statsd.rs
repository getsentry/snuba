use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use sentry_arroyo::metrics::{Metric, MetricType, MetricValue, Recorder};

use crate::config::EnvConfig;
use crate::metrics::global_tags::get_global_tags;

/// A metrics backend that uses `metrics-exporter-dogstatsd` to send metrics
/// to DogStatsD over a Unix domain socket. Adapts arroyo's [`Recorder`]
/// trait to the `metrics` crate facade installed by the exporter.
#[derive(Debug)]
pub struct DogStatsDBackend;

impl DogStatsDBackend {
    /// `socket_path` is passed to the exporter verbatim, so it must be a full DogStatsD
    /// remote address including the transport scheme, e.g. `unixgram:///run/dogstatsd.sock`
    /// (datagram) or `unix:///run/dogstatsd.sock` (stream). The scheme is supplied via the
    /// `SNUBA_DOGSTATSD_SOCKET_PATH` env var rather than hardcoded here, so the same value
    /// also works for the Python datadog client (which strips the scheme itself).
    pub fn new_uds(socket_path: &str, prefix: &str, tags: &[(&str, String)]) -> Self {
        let global_labels: Vec<Label> = tags
            .iter()
            .map(|(k, v)| Label::new(k.to_string(), v.clone()))
            .collect();

        DogStatsDBuilder::default()
            .with_remote_address(socket_path)
            .expect("invalid DogStatsD address")
            .set_global_prefix(prefix)
            .with_global_labels(global_labels)
            .send_histograms_as_distributions(true)
            .with_telemetry(true)
            .install()
            .expect("failed to install DogStatsD exporter");

        Self
    }
}

/// Build the DogStatsD metrics backend.
///
/// Metrics go to the local DogStatsD agent over the Unix domain socket configured by
/// `dogstatsd_socket_path` (`SNUBA_DOGSTATSD_SOCKET_PATH`), matching the gating in the
/// Python `create_metrics()`.
///
/// Returns `None` when no socket is configured, leaving metrics disabled.
pub fn create_dogstatsd_backend(
    env: &EnvConfig,
    prefix: &str,
    tags: &[(&str, String)],
) -> Option<DogStatsDBackend> {
    let socket_path = env.dogstatsd_socket_path.as_deref()?;

    // Tag every metric with the transport in use. UDS is now the only transport, but the
    // tag predates that and dashboards filter on it, so it is kept as a constant.
    let mut tags = tags.to_vec();
    tags.push(("dogstatsd_transport", "uds".to_owned()));
    Some(DogStatsDBackend::new_uds(socket_path, prefix, &tags))
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

        match metric.ty {
            MetricType::Counter => {
                let key = metrics::Key::from_parts(key, labels);
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
                let key = metrics::Key::from_parts(key, labels);
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
                let key = metrics::Key::from_parts(format!("{key}.distribution"), labels);
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
    fn timer_is_renamed_with_distribution_suffix() {
        use metrics_util::debugging::DebuggingRecorder;

        // Construct the backend directly (no exporter install), and capture emitted
        // metrics via a thread-local recorder so we don't touch the global one.
        let backend = DogStatsDBackend;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            backend.record_metric(metric!(Timer: "insertions.batch_write_ms", 30));
            backend.record_metric(metric!(Counter: "insertions.batch_write_msgs", 1));
        });

        let names: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(ck, _, _, _)| ck.key().name().to_string())
            .collect();

        // Timer -> renamed to a ".distribution" metric (existing convention)
        assert_eq!(names[0], "insertions.batch_write_ms.distribution");
        // Counter -> name unchanged.
        assert_eq!(names[1], "insertions.batch_write_msgs");
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn disabled_without_a_socket() {
        // No socket configured -> metrics disabled. Returning `None` here also means no
        // global recorder is installed, so this stays safe to assert in-process.
        let env = EnvConfig {
            dogstatsd_socket_path: None,
            ..Default::default()
        };
        assert!(create_dogstatsd_backend(&env, "snuba.consumer", &[]).is_none());
    }
}
