use core::fmt::{self, Display};
use std::time::Duration;

/// The Type of a Metric.
///
/// Counters, Gauges and Distributions are supported,
/// with more types to be added later.
#[non_exhaustive]
#[derive(Debug)]
pub enum MetricType {
    /// A counter metric, using the StatsD `c` type.
    Counter,
    /// A gauge metric, using the StatsD `g` type.
    Gauge,
    /// A timer metric, using the StatsD `ms` type.
    Timer,
    // Distribution,
    // Meter,
    // Histogram,
    // Set,
}

impl MetricType {
    /// Returns the StatsD metrics type.
    pub fn as_str(&self) -> &str {
        match self {
            MetricType::Counter => "c",
            MetricType::Gauge => "g",
            MetricType::Timer => "ms",
        }
    }
}

impl Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A Metric Value.
///
/// This supports various numeric values for now, but might gain support for
/// `Duration` and other types later on.
#[non_exhaustive]
#[derive(Debug)]
pub enum MetricValue {
    /// A signed value.
    I64(i64),
    /// An unsigned value.
    U64(u64),
    /// A floating-point value.
    F64(f64),
    /// A [`Duration`] value.
    Duration(Duration),
}

impl Display for MetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricValue::I64(v) => v.fmt(f),
            MetricValue::U64(v) => v.fmt(f),
            MetricValue::F64(v) => v.fmt(f),
            MetricValue::Duration(d) => d.as_millis().fmt(f),
        }
    }
}

macro_rules! into_metric_value {
    ($($from:ident),+ => $variant:ident) => {
        $(
            impl From<$from> for MetricValue {
                #[inline(always)]
                fn from(f: $from) -> Self {
                    Self::$variant(f.into())
                }
            }
        )+
    };
}

into_metric_value!(i8, i16, i32, i64 => I64);
into_metric_value!(u8, u16, u32, u64 => U64);
into_metric_value!(f32, f64 => F64);
into_metric_value!(Duration => Duration);

/// An alias for a list of Metric tags.
pub type MetricTags<'a> = &'a [(Option<&'a dyn Display>, &'a dyn Display)];

/// A fully types Metric.
///
/// Most importantly, the metric has a [`ty`](MetricType), a `key` and a [`value`](MetricValue).
/// It can also have a list of [`tags`](MetricTags).
///
/// This struct might change in the future, and one should construct it via
/// the [`metric!`](crate::metric) macro instead.
pub struct Metric<'a> {
    /// The key, or name, of the metric.
    pub key: &'a dyn Display,
    /// The type of metric.
    pub ty: MetricType,

    /// A list of tags for this metric.
    pub tags: MetricTags<'a>,
    /// The metrics value.
    pub value: MetricValue,

    #[doc(hidden)]
    pub __private: (),
}
