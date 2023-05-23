use std::{collections::HashMap, sync::{Mutex}};

use rust_arroyo::utils::metrics::MetricsClientTrait;

use super::abstract_backend::MetricsBackend;
use lazy_static::lazy_static;

lazy_static! {
    static ref METRICS: Mutex<HashMap<String, Vec<MetricCall>>> = {
        let m = HashMap::new();
        Mutex::new(m)
    };
}


pub struct MetricCall {
    _value: String,
    _tags: Vec<String>,
}

pub struct TestingMetricsBackend {
}

impl MetricsBackend for TestingMetricsBackend {
    fn events(
        &self,
        _title: &str,
        _text: &str,
        _alert_type: &str,
        _priority: &str,
        _tags: &[&str],
    ) {
        todo!()
    }
}

impl MetricsClientTrait for TestingMetricsBackend{
    fn counter(&self, name: &str, value: Option<i64>, tags: Option<HashMap<&str, &str>>, _sample_rate: Option<f64>) {
        let mut tags_vec = Vec::new();
        if let Some(tags) = tags {
            for (k, v) in tags {
                tags_vec.push(format!("{k}:{v}"));
            }
        }
        let mut metrics_map = METRICS.lock().unwrap();
        let metric = metrics_map.entry(name.to_string()).or_insert(Vec::new());
        metric.push(MetricCall {
            _value: value.unwrap().to_string(),
            _tags: tags_vec,
        });
    }

    fn gauge(&self, name: &str, value: u64, tags: Option<HashMap<&str, &str>>, _sample_rate: Option<f64>) {
        let mut tags_vec = Vec::new();
        if let Some(tags) = tags {
            for (k, v) in tags {
                tags_vec.push(format!("{k}:{v}"));
            }
        }
        let mut metrics_map = METRICS.lock().unwrap();
        let metric = metrics_map.entry(name.to_string()).or_insert(Vec::new());

        metric.push(MetricCall {
            _value: value.to_string(),
            _tags: tags_vec,
        });
    }

    fn time(&self, name: &str, value: u64, tags: Option<HashMap<&str, &str>>, _sample_rate: Option<f64>) {
        let mut tags_vec = Vec::new();
        if let Some(tags) = tags {
            for (k, v) in tags {
                tags_vec.push(format!("{k}:{v}"));
            }
        }
        let mut metrics_map = METRICS.lock().unwrap();
        let metric = metrics_map.entry(name.to_string()).or_insert(Vec::new());

        metric.push(MetricCall {
            _value: value.to_string(),
            _tags: tags_vec,
        });
    }

}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rust_arroyo::utils::metrics::{configure_metrics, MetricsClientTrait, self};

    use crate::utils::metrics::backends::testing::METRICS;


    #[test]
    fn test_testing_backend() {
        let testing_backend = super::TestingMetricsBackend {};

        let mut tags: HashMap<&str, &str> = HashMap::new();
        tags.insert("tag1", "value1");
        tags.insert("tag2", "value2");

        testing_backend.counter("test_counter", Some(1), Some(tags.clone()), None);
        testing_backend.gauge("test_gauge", 1, Some(tags.clone()), None);
        testing_backend.time("test_time", 1, Some(tags.clone()), None);
        assert!(METRICS.lock().unwrap().contains_key("test_counter"));
        assert!(METRICS.lock().unwrap().contains_key("test_gauge"));
        assert!(METRICS.lock().unwrap().contains_key("test_time"));

        // check configure_metrics writes to METRICS
        configure_metrics(testing_backend);
        metrics::time("c", 30, Some(HashMap::from([("tag3", "value3")])), None);
        assert!(METRICS.lock().unwrap().contains_key("c"));
    }
}
