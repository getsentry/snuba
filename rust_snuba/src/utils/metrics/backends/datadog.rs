use rust_arroyo::utils::metrics::{MetricsClientTrait};
use dogstatsd;

use super::abstract_backend::MetricsBackend;
pub struct DatadogMetricsBackend {
    client_sd: dogstatsd::Client,
    _tags: Vec<String>,
}

impl MetricsBackend for DatadogMetricsBackend {
    fn events(
        &self,
        title: &str,
        text: &str,
        _alert_type: &str,
        _priority: &str,
        tags: &[&str],
    ) {
        // TODO figure out how to send priority and alert_type
        self.client_sd.event(title, text, tags ).unwrap()
    }
}


impl DatadogMetricsBackend {
    pub fn new(host: String, port: u16 , tags:Vec<String>) -> Self {

        let host_port: String = format!("{host}:{port}");
        let options = dogstatsd::OptionsBuilder::new()
            .to_addr(host_port).build();

        let client = dogstatsd::Client::new(options).unwrap();
        DatadogMetricsBackend {
            client_sd: client,
            _tags: tags
        }
    }
}

impl MetricsClientTrait for DatadogMetricsBackend {
    fn counter(
        &self,
        key: &str,
        value: Option<i64>,
        tags: Option<std::collections::HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        if !self.should_sample(sample_rate) {
            return;
        }

        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{k}:{v}")).collect();

        match value {
            Some(v) => {
                self.client_sd.count(key, v, &tags_str).unwrap();
            }
            None => {
                self.client_sd.incr(key, tags_str).unwrap();
            }
        }


    }

    fn gauge(
        &self,
        key: &str,
        value: u64,
        tags: Option<std::collections::HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        if !self.should_sample(sample_rate) {
            return;
        }
        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{k}:{v}")).collect();
        self.client_sd.gauge(key, value.to_string(), tags_str).unwrap();
    }

    fn time(
        &self,
        key: &str,
        value: u64,
        tags: Option<std::collections::HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        if !self.should_sample(sample_rate) {
            return;
        }
        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{k}:{v}")).collect();
        self.client_sd.timing(key, value.try_into().unwrap(), tags_str).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rust_arroyo::utils::metrics::{configure_metrics, MetricsClientTrait, self};

    use crate::utils::metrics::backends::{datadog::DatadogMetricsBackend};


    #[test]
    fn test_testing_backend() {
        // default client sends metrics to statsd daemon on localhost:8125
        let client = dogstatsd::Client::new(dogstatsd::Options::default()).unwrap();
        let client_tags: Vec<String> = Vec::new();
        let testing_backend = DatadogMetricsBackend {
            client_sd: client,
            _tags: client_tags
        };

        let mut tags: HashMap<&str, &str> = HashMap::new();
        tags.insert("tag1", "value1");
        tags.insert("tag2", "value2");

        testing_backend.counter("test_counter", Some(1), Some(tags.clone()), None);
        testing_backend.gauge("test_gauge", 1, Some(tags.clone()), None);
        testing_backend.time("test_time", 1, Some(tags.clone()), None);

        // check configure_metrics writes to METRICS
        configure_metrics(testing_backend);
        metrics::time("c", 30, Some(HashMap::from([("tag3", "value3")])), None);

        // test constructor
        DatadogMetricsBackend::new("0.0.0.0".to_owned(), 8125, Vec::new())
            .counter("test_counter2", Some(2), Some(tags.clone()), None);
    }
}
