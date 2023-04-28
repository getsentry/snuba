use rust_arroyo::utils::metrics::{gauge, increment, init, time, MetricsClientTrait};
use dogstatsd;

use super::abstract_backend::MetricsBackend;
pub struct DatadogMetricsBackend {
    client_sd: dogstatsd::Client,
    tags: Vec<String>,
}

impl MetricsBackend for DatadogMetricsBackend {
    fn events(
        &self,
        title: &str,
        text: &str,
        alert_type: &str,
        priority: &str,
        tags: &[&str],
    ) {
        // TODO figure out how to send priority and alert_type
        self.client_sd.event(title, text, tags );
    }
}


impl DatadogMetricsBackend {
    pub fn new(host: String, port: u16 , tags:Vec<String>) -> Self {

        let host_port: String = format!("{}:{}", host, port);
        let built_options = dogstatsd::OptionsBuilder::new()
            .to_addr(host_port).build();

        let client = dogstatsd::Client::new(dogstatsd::Options::default()).unwrap();
        DatadogMetricsBackend {
            client_sd: client,
            tags
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
        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{}:{}", k, v)).collect();

        match value {
            Some(v) => {
                for _ in 0..v.abs() {
                    if v < 0 {
                        self.client_sd.decr(key, &tags_str).unwrap();
                    } else {
                        self.client_sd.incr(key, &tags_str).unwrap();
                    }
                }
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
        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
        let res = self.client_sd.gauge(key, value.to_string(), tags_str).unwrap();
        println!("ok = {:?}", res)
    }

    fn time(
        &self,
        key: &str,
        value: u64,
        tags: Option<std::collections::HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
        self.client_sd.timing(key, value.try_into().unwrap(), tags_str).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dogstatsd::Options;
    use rust_arroyo::utils::metrics::{configure_metrics, MetricsClientTrait, self};

    use crate::utils::metrics::backends::{datadog::DatadogMetricsBackend};


    #[test]
    fn test_testing_backend() {

        let custom_options = Options::new("0.0.0.0:9000", "0.0.0.0:8125", "analytics", vec!(String::new()));
        // let client = dogstatsd::Client::new(dogstatsd::Options::default()).unwrap();
        let client = dogstatsd::Client::new(custom_options).unwrap();
        let client_tags: Vec<String> = Vec::new();
        let testing_backend = DatadogMetricsBackend {
            client_sd: client,
            tags: client_tag
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
    }
}
