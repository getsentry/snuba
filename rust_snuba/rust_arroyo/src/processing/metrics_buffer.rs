use crate::utils::metrics::{get_metrics, Metrics};
use core::fmt::Debug;
use std::collections::BTreeMap;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct MetricsBuffer {
    metrics: Arc<Mutex<Box<dyn Metrics>>>,
    timers: BTreeMap<String, Duration>,
    last_flush: Instant,
}

impl MetricsBuffer {
    // A pretty shitty metrics buffer that only handles timing metrics
    // and flushes them every second. Needs to be flush()-ed on shutdown
    // Doesn't support tags
    // Basically the same as https://github.com/getsentry/arroyo/blob/83f5f54e59892ad0b62946ef35d2daec3b561b10/arroyo/processing/processor.py#L80-L112
    // We may want to replace this with the statsdproxy aggregation step.
    pub fn new() -> Self {
        Self {
            metrics: get_metrics(),
            timers: BTreeMap::new(),
            last_flush: Instant::now(),
        }
    }

    pub fn incr_timing(&mut self, metric: &str, duration: Duration) {
        if let Some(value) = self.timers.get_mut(metric) {
            *value += duration;
        } else {
            self.timers.insert(metric.to_string(), duration);
        }
        self.throttled_record();
    }

    pub fn flush(&mut self) {
        let timers = mem::take(&mut self.timers);
        for (metric, duration) in timers {
            self.metrics
                .timing(&metric, duration.as_millis() as u64, None);
        }
        self.last_flush = Instant::now();
    }

    fn throttled_record(&mut self) {
        if self.last_flush.elapsed() > Duration::from_secs(1) {
            self.flush();
        }
    }
}

impl Default for MetricsBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[test]
    fn metrics_buffer() {
        #[derive(Debug)]
        struct Test {
            data: Vec<String>,
        }

        impl Metrics for Test {
            fn increment(
                &mut self,
                _key: &str,
                _value: Option<i64>,
                _tags: Option<HashMap<&str, &str>>,
            ) {
            }

            fn gauge(&mut self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}

            fn timing(&mut self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
                // self.data.append((key.into(), value.into(), tags.into()))
                self.data.push("asdf".to_string());
            }
        }
    }
}
