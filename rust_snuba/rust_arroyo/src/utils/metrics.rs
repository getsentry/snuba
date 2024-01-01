use core::fmt::Debug;
use std::collections::HashMap;
use std::sync::OnceLock;

pub static METRICS: OnceLock<Box<dyn Metrics>> = OnceLock::new();
pub type BoxMetrics = &'static dyn Metrics;

pub trait Metrics: Debug + Send + Sync {
    fn increment(&self, key: &str, value: i64, tags: Option<HashMap<&str, &str>>);

    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>);

    fn timing(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>);
}

impl Metrics for Box<dyn Metrics> {
    fn increment(&self, key: &str, value: i64, tags: Option<HashMap<&str, &str>>) {
        (**self).increment(key, value, tags)
    }
    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        (**self).gauge(key, value, tags)
    }
    fn timing(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        (**self).timing(key, value, tags)
    }
}

#[derive(Debug)]
struct Noop;

impl Metrics for Noop {
    fn increment(&self, _key: &str, _value: i64, _tags: Option<HashMap<&str, &str>>) {}

    fn gauge(&self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}

    fn timing(&self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}
}

pub fn configure_metrics<M>(metrics: M)
where
    M: Metrics + 'static,
{
    // Metrics can only be configured once
    METRICS
        .set(Box::new(metrics))
        .expect("Metrics already configured");
}

pub fn get_metrics() -> BoxMetrics {
    METRICS.get().map(|b| &**b).unwrap_or(&Noop)
}
