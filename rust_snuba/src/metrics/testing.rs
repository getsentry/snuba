#![allow(dead_code)]

use cadence::prelude::*;
use cadence::{Metric, NopMetricSink, StatsdClient};
use cadence::{MetricBuilder, MetricError};

use rust_arroyo::utils::metrics::Metrics as ArroyoMetrics;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct TestingMetricsBackend {
    client: StatsdClient,
    metric_calls: Arc<Mutex<Vec<String>>>,
}

impl TestingMetricsBackend {
    fn new(prefix: &str) -> Self {
        let client = StatsdClient::from_sink(prefix, NopMetricSink);

        Self {
            client,
            metric_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn clear(&self) {
        self.metric_calls.lock().unwrap().clear();
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

impl ArroyoMetrics for TestingMetricsBackend {
    fn increment(&self, key: &str, value: i64, tags: Option<HashMap<&str, &str>>) {
        let builder = self.client.count_with_tags(key, value);

        let res = self
            .send_with_tags(builder, tags)
            .unwrap()
            .as_metric_str()
            .to_owned();

        self.metric_calls.lock().unwrap().push(res);
    }

    fn gauge(&mut self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        let builder = self.client.gauge_with_tags(key, value);

        let res = self
            .send_with_tags(builder, tags)
            .unwrap()
            .as_metric_str()
            .to_owned();

        self.metric_calls.lock().unwrap().push(res);
    }

    fn timing(&mut self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        let builder = self.client.time_with_tags(key, value);

        let res = self
            .send_with_tags(builder, tags)
            .unwrap()
            .as_metric_str()
            .to_owned();

        self.metric_calls.lock().unwrap().push(res);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn testing_metrics_backend() {
        let backend = TestingMetricsBackend::new("snuba.rust-consumer");

        backend.increment("a", 1, Some(HashMap::from([("tag1", "value1")])));
        backend.gauge("b", 20, Some(HashMap::from([("tag2", "value2")])));
        backend.timing("c", 30, Some(HashMap::from([("tag3", "value3")])));

        let expected = vec![
            "snuba.rust-consumer.a:1|c|#tag1:value1",
            "snuba.rust-consumer.b:20|g|#tag2:value2",
            "snuba.rust-consumer.c:30|ms|#tag3:value3",
        ];

        assert_eq!(*backend.metric_calls.lock().unwrap(), expected);
    }
}
