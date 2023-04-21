// use crate::utils::metrics::backends::MetricsBackend;

use rust_arroyo::utils::metrics::{gauge, increment, init, time, MetricsClientTrait};
use dogstatsd;
use cadence::StatsdClient;

use super::abstract_backend::MetricsBackend;
pub struct DatadogMetricsBackend {
    client_sd: dogstatsd::Client,
    host: String,
    port: u16,
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
            host,
            port,
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
                for i in 0..v.abs() {
                    if v < 0 {
                        self.client_sd.decr(key, &tags_str).unwrap();
                    } else {
                        self.client_sd.incr(key, &tags_str).unwrap();
                    }
                }
            }
            None => {
                self.client_sd.incr(key, tags_str);
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
        self.client_sd.gauge(key, value.to_string(), tags_str).unwrap();
    }

    fn time(
        &self,
        key: &str,
        value: u64,
        tags: Option<std::collections::HashMap<&str, &str>>,
        sample_rate: Option<f64>,
    ) {
        let tags_str: Vec<String> = tags.unwrap().iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
        self.client_sd.timing(key, value.try_into().unwrap(), tags_str);
    }
}
