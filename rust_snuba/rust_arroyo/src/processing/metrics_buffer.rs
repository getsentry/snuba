use crate::utils::metrics::{get_metrics, Metrics};
use crate::utils::timing::Deadline;
use core::fmt::Debug;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct MetricsBuffer {
    metrics: Arc<dyn Metrics>,
    timers: BTreeMap<String, Duration>,
    gauges: BTreeMap<String, u64>,
    flush_deadline: Deadline,
}

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

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
            gauges: BTreeMap::new(),
            flush_deadline: Deadline::new(FLUSH_INTERVAL),
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

    pub fn gauge(&mut self, metric: &str, value: u64) {
        if !self.gauges.contains_key(metric) {
            self.gauges.insert(metric.to_string(), value);
        }
        self.throttled_record();
    }

    pub fn flush(&mut self) {
        let timers = mem::take(&mut self.timers);
        for (metric, duration) in timers {
            self.metrics
                .timing(&metric, duration.as_millis() as u64, None);
        }
        let gauges = mem::take(&mut self.gauges);
        for (metric, value) in gauges {
            self.metrics.gauge(&metric, value, None);
        }

        self.flush_deadline.restart();
    }

    fn throttled_record(&mut self) {
        if self.flush_deadline.has_elapsed() {
            self.flush();
        }
    }
}

impl Default for MetricsBuffer {
    fn default() -> Self {
        Self::new()
    }
}
