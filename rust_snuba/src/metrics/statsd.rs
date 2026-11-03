use metrics::{
    Counter, Gauge, Histogram, Key, KeyName, Label, Metadata, Recorder, SharedString, Unit,
};
use metrics_exporter_dogstatsd::DogStatsDBuilder;

use crate::config::EnvConfig;
use crate::metrics::global_tags::get_global_tags;

/// Wraps the DogStatsD recorder to keep the conventions snuba's dashboards
/// depend on, for both snuba's and arroyo's metrics:
/// - histograms are renamed with a `.distribution` suffix
/// - the dynamic global tags (`set_global_tag`) are attached at registration
struct SnubaRecorder<R> {
    inner: R,
}

fn with_global_tags(key: &Key) -> Key {
    let mut labels: Vec<Label> = key.labels().cloned().collect();
    for (k, v) in get_global_tags() {
        labels.push(Label::new(k, v));
    }
    Key::from_parts(key.name().to_owned(), labels)
}

impl<R: Recorder> Recorder for SnubaRecorder<R> {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, desc: SharedString) {
        self.inner.describe_counter(key, unit, desc)
    }
    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, desc: SharedString) {
        self.inner.describe_gauge(key, unit, desc)
    }
    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, desc: SharedString) {
        self.inner.describe_histogram(key, unit, desc)
    }
    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.inner
            .register_counter(&with_global_tags(key), metadata)
    }
    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.inner.register_gauge(&with_global_tags(key), metadata)
    }
    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let key = with_global_tags(key);
        let renamed = Key::from_parts(
            format!("{}.distribution", key.name()),
            key.labels().cloned().collect::<Vec<_>>(),
        );
        self.inner.register_histogram(&renamed, metadata)
    }
}

/// Install the DogStatsD exporter as the global metrics-rs recorder.
///
/// Metrics go to the local DogStatsD agent over the Unix domain socket configured by
/// `dogstatsd_socket_path`; the address is passed to the exporter verbatim, scheme
/// included, as documented on `settings.DOGSTATSD_SOCKET_PATH` (both runtimes consume the
/// same value).
///
/// Does nothing when no socket is configured, leaving metrics disabled — the same gating
/// as the Python `create_metrics()`.
pub fn install_dogstatsd(env: &EnvConfig, prefix: &str, tags: &[(&str, String)]) {
    let Some(socket_path) = env.dogstatsd_socket_path.as_deref() else {
        return;
    };

    let mut global_labels: Vec<Label> = tags
        .iter()
        .map(|(k, v)| Label::new(k.to_string(), v.clone()))
        .collect();
    // UDS is the only transport, so this tag is now constant. It is kept because Datadog
    // dashboards filter on it; delete it once no dashboard references dogstatsd_transport.
    global_labels.push(Label::new("dogstatsd_transport", "uds"));

    let recorder = DogStatsDBuilder::default()
        .with_remote_address(socket_path)
        .expect("invalid DogStatsD address")
        .set_global_prefix(prefix)
        .with_global_labels(global_labels)
        .send_histograms_as_distributions(true)
        .with_telemetry(true)
        .build()
        .expect("failed to build DogStatsD exporter");

    metrics::set_global_recorder(SnubaRecorder { inner: recorder })
        .expect("failed to install DogStatsD exporter");
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::DebuggingRecorder;

    use super::*;
    use crate::metrics::global_tags::set_global_tag;

    #[test]
    fn keeps_dashboard_conventions() {
        set_global_tag("parity_test.min_partition".to_owned(), "3".to_owned());

        let debugging = DebuggingRecorder::new();
        let snapshotter = debugging.snapshotter();
        let recorder = SnubaRecorder { inner: debugging };

        metrics::with_local_recorder(&recorder, || {
            crate::timer!("insertions.batch_write_ms", 30u64);
            crate::counter!("insertions.batch_write_msgs", 1u64);
            crate::gauge!("insertions.batch_flush_msgs", 5i64);
            // what arroyo now emits directly, bypassing snuba's macros
            metrics::histogram!("arroyo.consumer.latency").record(12.0);
        });

        let mut seen: Vec<(String, bool)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(ck, _, _, _)| {
                let tagged = ck
                    .key()
                    .labels()
                    .any(|l| l.key() == "parity_test.min_partition" && l.value() == "3");
                (ck.key().name().to_string(), tagged)
            })
            .collect();
        seen.sort();

        assert_eq!(
            seen,
            vec![
                ("arroyo.consumer.latency.distribution".to_string(), true),
                ("insertions.batch_flush_msgs".to_string(), true),
                ("insertions.batch_write_ms.distribution".to_string(), true),
                ("insertions.batch_write_msgs".to_string(), true),
            ]
        );
    }
}
