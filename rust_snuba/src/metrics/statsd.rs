#![allow(dead_code)]

use cadence::prelude::*;
use cadence::{BufferedUdpMetricSink, MetricBuilder, MetricError, QueuingMetricSink, StatsdClient};
use rust_arroyo::utils::metrics::Metrics as ArroyoMetrics;
use std::collections::HashMap;
use std::net::UdpSocket;

#[derive(Debug)]
pub struct StatsDBackend {
    client: StatsdClient,
}

impl StatsDBackend {
    pub fn new(host: &str, port: u16, prefix: &str, global_tags: HashMap<&str, &str>) -> Self {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let sink_addr = (host, port);
        let buffered = BufferedUdpMetricSink::from(sink_addr, socket).unwrap();
        let queuing_sink = QueuingMetricSink::from(buffered);

        let mut client_builder = StatsdClient::builder(prefix, queuing_sink);
        for (k, v) in global_tags {
            client_builder = client_builder.with_tag(k, v);
        }

        Self {
            client: client_builder.build(),
        }
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
        if let Err(e) = self.send_with_tags(self.client.count_with_tags(key, value), tags) {
            log::debug!("Error sending metric: {}", e);
        }
    }

    fn gauge(&mut self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        if let Err(e) = self.send_with_tags(self.client.gauge_with_tags(key, value), tags) {
            log::debug!("Error sending metric: {}", e);
        }
    }

    fn timing(&mut self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        if let Err(e) = self.send_with_tags(self.client.time_with_tags(key, value), tags) {
            log::debug!("Error sending metric: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn statsd_metric_backend() {
        let mut backend =
            StatsDBackend::new("0.0.0.0", 8125, "test", HashMap::from([("env", "prod")]));

        backend.increment("a", 1, Some(HashMap::from([("tag1", "value1")])));
        backend.gauge("b", 20, Some(HashMap::from([("tag2", "value2")])));
        backend.timing("c", 30, Some(HashMap::from([("tag3", "value3")])));
    }
}
