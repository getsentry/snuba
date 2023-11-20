use core::fmt::Debug;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

pub static METRICS: OnceLock<Arc<Mutex<Box<dyn Metrics>>>> = OnceLock::new();
pub type BoxMetrics = Arc<Mutex<Box<dyn Metrics>>>;

pub trait Metrics: Debug + Send + Sync {
    fn increment(&self, key: &str, value: i64, tags: Option<HashMap<&str, &str>>);

    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>);

    fn timing(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>);
}

impl Metrics for BoxMetrics {
    fn increment(&self, key: &str, value: i64, tags: Option<HashMap<&str, &str>>) {
        self.as_ref().lock().unwrap().increment(key, value, tags)
    }
    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        self.as_ref().lock().unwrap().gauge(key, value, tags)
    }
    fn timing(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        self.as_ref().lock().unwrap().timing(key, value, tags)
    }
}

#[derive(Debug)]
struct Noop {}

impl Metrics for Noop {
    fn increment(&self, _key: &str, _value: i64, _tags: Option<HashMap<&str, &str>>) {}

    fn gauge(&self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}

    fn timing(&self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}
}

pub fn configure_metrics(metrics: Box<dyn Metrics>) {
    // Metrics can only be configured once
    METRICS
        .set(Arc::new(Mutex::new(metrics)))
        .expect("Metrics already configured");
}

pub fn get_metrics() -> BoxMetrics {
    match METRICS.get() {
        Some(metrics) => metrics.clone(),
        None => Arc::new(Mutex::new(Box::new(Noop {}))),
    }
}
