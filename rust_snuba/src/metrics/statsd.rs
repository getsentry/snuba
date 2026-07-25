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

/// Install the DogStatsD exporter and return the backend adapting arroyo onto it.
///
/// Metrics go to the local DogStatsD agent over the Unix domain socket configured by
/// `dogstatsd_socket_path`; the address is passed to the exporter verbatim, scheme
/// included, as documented on `settings.DOGSTATSD_SOCKET_PATH` (both runtimes consume the
/// same value).
///
/// Returns `None` when no socket is configured, leaving metrics disabled — the same gating
/// as the Python `create_metrics()`.
pub fn create_dogstatsd_backend(
    env: &EnvConfig,
    prefix: &str,
    tags: &[(&str, String)],
) -> Option<DogStatsDBackend> {
    let socket_path = env.dogstatsd_socket_path.as_deref()?;

    let mut global_labels: Vec<Label> = tags
        .iter()
        .map(|(k, v)| Label::new(k.to_string(), v.clone()))
        .collect();
    // UDS is the only transport, so this tag is now constant. It is kept because Datadog
    // dashboards filter on it; delete it once no dashboard references dogstatsd_transport.
    global_labels.push(Label::new("dogstatsd_transport", "uds"));

    DogStatsDBuilder::default()
        .with_remote_address(socket_path)
        .expect("invalid DogStatsD address")
        .set_global_prefix(prefix)
        .with_global_labels(global_labels)
        .send_histograms_as_distributions(true)
        .with_telemetry(true)
        .install()
        .expect("failed to install DogStatsD exporter");

    Some(DogStatsDBackend)
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
        // Returning `None` short-circuits before installing the exporter, so asserting this
        // in-process leaves the global recorder untouched.
        let env = EnvConfig::default();
        assert!(env.dogstatsd_socket_path.is_none());
        assert!(create_dogstatsd_backend(&env, "snuba.consumer", &[]).is_none());
    }
}
