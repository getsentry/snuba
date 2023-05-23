use cadence::{
    BufferedUdpMetricSink, Counted, Gauged, MetricBuilder, QueuingMetricSink, StatsdClient, Timed,
};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashMap;
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::Arc;

pub trait MetricsClientTrait: Send + Sync  {

    fn should_sample(&self, sample_rate: Option<f64>) -> bool {
        rand::thread_rng().gen_range(0.0..=1.0) < sample_rate.unwrap_or(1.0)
    }

    fn counter(
        &self,
        key: &str,
        value: Option<i64>,
        tags: Option<HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    );

    fn gauge(
        &self,
        key: &str,
        value: u64,
        tags: Option<HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    );

    fn time(
        &self,
        key: &str,
        value: u64,
        tags: Option<HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    );
}

pub struct MetricsClient {
    statsd_client: StatsdClient,
    prefix: String,
}

impl MetricsClientTrait for MetricsClient {
    fn counter(
        &self,
        key: &str,
        value: Option<i64>,
        tags: Option<HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        if !self.should_sample(sample_rate) {
            return;
        }

        let count_value = value.unwrap_or(1);

        if let Some(tags) = tags {
            let metric_builder = self.statsd_client.count_with_tags(key, count_value);
            self.send_with_tags(metric_builder, tags);
        } else {
            let result = self.statsd_client.count(key, count_value);
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {key}: {_err}")
                }
            }
        }
    }

    fn gauge(
        &self,
        key: &str,
        value: u64,
        tags: Option<HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        if !self.should_sample(sample_rate) {
            return;
        }

        if let Some(tags) = tags {
            let metric_builder = self.statsd_client.gauge_with_tags(key, value);
            self.send_with_tags(metric_builder, tags);
        } else {
            let result = self.statsd_client.gauge(key, value);
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {key}: {_err}")
                }
            }
        }
    }

    fn time(
        &self,
        key: &str,
        value: u64,
        tags: Option<HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        if !self.should_sample(sample_rate) {
            return;
        }

        if let Some(tags) = tags {
            let metric_builder = self.statsd_client.time_with_tags(key, value);
            self.send_with_tags(metric_builder, tags);
        } else {
            let result = self.statsd_client.time(key, value);
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {key}: {_err}")
                }
            }
        }
    }
}

impl MetricsClient {

    fn send_with_tags<'t, T: cadence::Metric + From<String>>(
        &self,
        mut metric_builder: MetricBuilder<'t, '_, T>,
        tags: HashMap<&'t str, &'t str>,
    ) {
        for (key, value) in tags {
            metric_builder = metric_builder.with_tag(key, value);
        }
        let result = metric_builder.try_send();
        match result {
            Ok(_) => {}
            Err(_err) => {
                println!("Failed to send metric with tags: {_err}")
            }
        }
    }
}

lazy_static! {
    static ref METRICS_CLIENT: RwLock<Option<Arc<dyn MetricsClientTrait>>> = RwLock::new(None);
}

const METRICS_MAX_QUEUE_SIZE: usize = 1024;

pub fn init<A: ToSocketAddrs>(prefix: &str, host: A) {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();

    let buffered_udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
    let queuing_sink = QueuingMetricSink::with_capacity(buffered_udp_sink, METRICS_MAX_QUEUE_SIZE);
    // If metrics need to be sent out immediately without waiting then use the udp_sink below.
    //let udp_sink = UdpMetricSink::from(host, socket).unwrap();
    let statsd_client = StatsdClient::from_sink(prefix, queuing_sink);

    let metrics_client = MetricsClient {
        statsd_client,
        prefix: String::from(prefix),
    };
    println!("Emitting metrics with prefix {}", metrics_client.prefix);
    *METRICS_CLIENT.write() = Some(Arc::new(metrics_client));
}
pub fn configure_metrics(metrics_client: impl MetricsClientTrait + 'static){
    *METRICS_CLIENT.write() = Some(Arc::new(metrics_client));
}

// TODO: Remove cloning METRICS_CLIENT each time this is called using thread local storage.
pub fn increment(
    key: &str,
    value: Option<i64>,
    tags: Option<HashMap<&str, &str>>,
    sample_rate: Option<f64>,
) {
    METRICS_CLIENT
        .read()
        .clone()
        .unwrap()
        .counter(key, value, tags, sample_rate);
}

// TODO: Remove cloning METRICS_CLIENT each time this is called using thread local storage.
pub fn gauge(key: &str, value: u64, tags: Option<HashMap<&str, &str>>, sample_rate: Option<f64>) {
    METRICS_CLIENT
        .read()
        .clone()
        .unwrap()
        .gauge(key, value, tags, sample_rate);
}

// TODO: Remove cloning METRICS_CLIENT each time this is called using thread local storage.
pub fn time(key: &str, value: u64, tags: Option<HashMap<&str, &str>>, sample_rate: Option<f64>) {
    METRICS_CLIENT
        .read()
        .clone()
        .unwrap()
        .time(key, value, tags, sample_rate);
}

#[cfg(test)]
mod tests {
    use crate::utils::metrics::{gauge, increment, init, time, METRICS_CLIENT};
    use std::collections::HashMap;

    #[test]
    fn test_metrics() {
        init("my_host", "0.0.0.0:8125");

        assert!(!METRICS_CLIENT
            .read()
            .clone()
            .unwrap()
            .should_sample(Some(0.0)),);

        assert!(METRICS_CLIENT
            .read()
            .clone()
            .unwrap()
            .should_sample(Some(1.0)),);

        increment(
            "a",
            Some(1),
            Some(HashMap::from([("tag1", "value1")])),
            Some(1.0),
        );
        gauge(
            "b",
            20,
            Some(HashMap::from([("tag2", "value2"), ("tag4", "value4")])),
            None,
        );
        time("c", 30, Some(HashMap::from([("tag3", "value3")])), None);
    }
}
