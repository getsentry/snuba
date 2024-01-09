use std::collections::HashMap;

use cadence::prelude::*;
use cadence::{MetricBuilder, MetricError, StatsdClient};
use rust_arroyo::utils::metrics::Metrics as ArroyoMetrics;
use statsdproxy::cadence::StatsdProxyMetricSink;
use statsdproxy::config::AggregateMetricsConfig;
use statsdproxy::middleware::aggregate::AggregateMetrics;
use statsdproxy::middleware::Upstream;

use crate::metrics::global_tags::AddGlobalTags;

#[derive(Debug)]
pub struct StatsDBackend {
    client: StatsdClient,
}

impl StatsDBackend {
    pub fn new(host: &str, port: u16, prefix: &str) -> Self {
        let upstream_addr = format!("{}:{}", host, port);
        let aggregator_sink = StatsdProxyMetricSink::new(move || {
            let next_step = Upstream::new(upstream_addr.clone()).unwrap();

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

        let client = StatsdClient::builder(prefix, aggregator_sink).build();
        Self { client }
    }

    fn send_with_tags<'t, T: cadence::Metric + From<String>>(
        &self,
        mut builder: MetricBuilder<'t, '_, T>,
        tags: Option<HashMap<&'t str, &'t str>>,
    ) -> Result<T, MetricError> {
        if let Some(t) = tags {
            for (key, value) in t {
                builder = builder.with_tag(key, value);
            }
        }

        builder.try_send()
    }
}

impl ArroyoMetrics for StatsDBackend {
    fn increment(&self, key: &str, value: i64, tags: Option<HashMap<&str, &str>>) {
        if let Err(error) = self.send_with_tags(self.client.count_with_tags(key, value), tags) {
            tracing::debug!(%error, "Error sending metric");
        }
    }

    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        if let Err(error) = self.send_with_tags(self.client.gauge_with_tags(key, value), tags) {
            tracing::debug!(%error, "Error sending metric");
        }
    }

    fn timing(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        if let Err(error) = self.send_with_tags(self.client.time_with_tags(key, value), tags) {
            tracing::debug!(%error, "Error sending metric");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn statsd_metric_backend() {
        let backend = StatsDBackend::new("0.0.0.0", 8125, "test");

        backend.increment("a", 1, Some(HashMap::from([("tag1", "value1")])));
        backend.gauge("b", 20, Some(HashMap::from([("tag2", "value2")])));
        backend.timing("c", 30, Some(HashMap::from([("tag3", "value3")])));
    }
}
