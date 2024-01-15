use std::cell::RefCell;
use std::fmt::{Debug, Display, Write};

use super::{Metric, Recorder};

thread_local! {
    static STRING_BUFFER: RefCell<String> = const { RefCell::new(String::new()) };
}

/// A generic sink used by the [`StatsdRecorder`].
pub trait MetricSink {
    /// Emits a StatsD-formatted `metric`.
    fn emit(&self, metric: &str);
}

/// A recorder emitting StatsD-formatted [`Metric`]s to a configured [`MetricSink`].
pub struct StatsdRecorder<S> {
    prefix: String,
    sink: S,
    tags: String,
}

impl<S> Debug for StatsdRecorder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsdRecorder")
            .field("prefix", &self.prefix)
            .field("formatted tags", &self.tags)
            .finish_non_exhaustive()
    }
}

impl<S> StatsdRecorder<S> {
    /// Creates a new Recorder with the given `prefix` and `sink`.
    ///
    /// The recorder will emit [`Metric`]s to formatted in `statsd` format to the
    /// configured [`MetricSink`].
    pub fn new(prefix: &str, sink: S) -> Self {
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}.", prefix.trim_end_matches('.'))
        };
        Self {
            prefix,
            sink,
            tags: String::new(),
        }
    }

    fn write_tag(mut self, key: Option<&dyn Display>, value: &dyn Display) -> Self {
        let t = &mut self.tags;
        if t.is_empty() {
            t.push_str("|#");
        } else {
            t.push(',');
        }

        if let Some(key) = key {
            let _ = write!(t, "{key}");
            t.push(':');
        }
        let _ = write!(t, "{value}");

        self
    }

    /// Add a global tag (as key/value) to this Recorder.
    pub fn with_tag(self, key: impl Display, value: impl Display) -> Self {
        self.write_tag(Some(&key), &value)
    }

    /// Add a global tag (as a single value) to this Recorder.
    pub fn with_tag_value(self, value: impl Display) -> Self {
        self.write_tag(None, &value)
    }

    fn write_metric(&self, metric: Metric<'_>, s: &mut String) {
        s.push_str(&self.prefix);
        metric.write_base_metric(s);

        s.push_str(&self.tags);
        if !metric.tags.is_empty() {
            if self.tags.is_empty() {
                s.push_str("|#");
            } else {
                s.push(',');
            }

            metric.write_tags(s);
        }
    }
}

impl<S: MetricSink> Recorder for StatsdRecorder<S> {
    fn record_metric(&self, metric: Metric<'_>) {
        STRING_BUFFER.with_borrow_mut(|s| {
            s.clear();
            s.reserve(256);

            self.write_metric(metric, s);

            self.sink.emit(s);
        });
    }
}

impl Metric<'_> {
    pub(crate) fn write_base_metric(&self, s: &mut String) {
        let _ = write!(s, "{}:{}|", self.key, self.value);
        s.push_str(self.ty.as_str());
    }

    pub(crate) fn write_tags(&self, s: &mut String) {
        for (i, &(key, value)) in self.tags.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            if let Some(key) = key {
                let _ = write!(s, "{key}");
                s.push(':');
            }
            let _ = write!(s, "{value}");
        }
    }
}
