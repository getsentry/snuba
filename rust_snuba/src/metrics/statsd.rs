use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use sentry_arroyo::metrics::{Metric, MetricType, MetricValue, Recorder};

/// A metrics backend that uses `metrics-exporter-dogstatsd` to send metrics
/// to DogStatsD over UDP or Unix domain sockets. Adapts arroyo's [`Recorder`]
/// trait to the `metrics` crate facade installed by the exporter.
#[derive(Debug)]
pub struct DogStatsDBackend;

impl DogStatsDBackend {
    pub fn new_udp(host: &str, port: u16, prefix: &str, tags: &[(&str, String)]) -> Self {
        let addr = format!("{}:{}", host, port);
        Self::build(&addr, prefix, tags)
    }

    pub fn new_uds(socket_path: &str, prefix: &str, tags: &[(&str, String)]) -> Self {
        let addr = format!("unixgram://{}", socket_path);
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
            .install()
            .expect("failed to install DogStatsD exporter");

        Self
    }
}

impl Recorder for DogStatsDBackend {
    fn record_metric(&self, metric: Metric<'_>) {
        let key: metrics::SharedString = metric.key.to_string().into();
        let labels: Vec<Label> = metric
            .tags
            .iter()
            .map(|(k, v)| Label::new(k.to_string(), v.to_string()))
            .collect();
        let metadata = metrics::Metadata::new(module_path!(), metrics::Level::INFO, None);
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
}
