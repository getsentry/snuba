use rust_arroyo::metrics::{Metric, MetricSink, Recorder, StatsdRecorder};
use statsdproxy::cadence::StatsdProxyMetricSink;
use statsdproxy::config::{AggregateMetricsConfig, SampleConfig};
use statsdproxy::middleware::aggregate::AggregateMetrics;
use statsdproxy::middleware::mirror::Mirror;
use statsdproxy::middleware::sample::Sample;
use statsdproxy::middleware::sentry::Sentry;
use statsdproxy::middleware::upstream::Upstream;

use crate::metrics::global_tags::AddGlobalTags;

#[derive(Debug)]
pub struct StatsDBackend {
    recorder: StatsdRecorder<Wrapper>,
}

impl Recorder for StatsDBackend {
    fn record_metric(&self, metric: Metric<'_>) {
        self.recorder.record_metric(metric)
    }
}

struct Wrapper(Box<dyn cadence::MetricSink + Send + Sync + 'static>);

impl MetricSink for Wrapper {
    fn emit(&self, metric: &str) {
        let _ = self.0.emit(metric);
    }
}

impl StatsDBackend {
    pub fn new(host: &str, port: u16, prefix: &str, ddm_metrics_sample_rate: f64) -> Self {
        let upstream_addr = format!("{}:{}", host, port);
        let aggregator_sink = StatsdProxyMetricSink::new(move || {
            let next_step = Upstream::new(upstream_addr.clone()).unwrap();

            let next_step_sentry = Sample::new(
                SampleConfig {
                    sample_rate: ddm_metrics_sample_rate,
                },
                Sentry::new(),
            );

            let next_step = Mirror::new(next_step, next_step_sentry);

            // adding global tags *after* aggregation is more performant than trying to do the same
            // in cadence, as it means more bytes and more memory to deal with in
            // AggregateMetricsConfig
            let next_step = AddGlobalTags::new(next_step);

            let config = AggregateMetricsConfig {
                aggregate_counters: true,
                flush_offset: 0,
                flush_interval: 1,
                aggregate_gauges: true,
                max_map_size: None,
            };
            AggregateMetrics::new(config, next_step)
        });

        let recorder = StatsdRecorder::new(prefix, Wrapper(Box::new(aggregator_sink)));

        Self { recorder }
    }
}

#[cfg(test)]
mod tests {
    use rust_arroyo::metric;

    use super::*;

    #[test]
    fn statsd_metric_backend() {
        let backend = StatsDBackend::new("0.0.0.0", 8125, "test", 0.0);

        backend.record_metric(metric!(Counter: "a", 1, "tag1" => "value1"));
        backend.record_metric(metric!(Gauge: "b", 20, "tag2" => "value2"));
        backend.record_metric(metric!(Timer: "c", 30, "tag3" => "value3"));
    }
}
