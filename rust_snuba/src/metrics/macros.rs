//! snuba's metric macros. Same call syntax as the arroyo macros snuba used before
//! arroyo moved to metrics-rs, so call sites only change their import.

use std::time::Duration;

#[doc(hidden)]
pub trait MetricValue {
    fn to_f64(self) -> f64;
}

macro_rules! impl_metric_value {
    ($($t:ty),*) => { $(impl MetricValue for $t { fn to_f64(self) -> f64 { self as f64 } })* };
}
impl_metric_value!(i32, i64, u32, u64, usize, f64);

impl MetricValue for Duration {
    fn to_f64(self) -> f64 {
        self.as_millis() as f64
    }
}

#[macro_export]
macro_rules! counter {
    ($key:expr) => {
        $crate::counter!($key, 1u64)
    };
    ($key:expr, $value:expr $(, $tag_key:expr => $tag_val:expr)* $(,)?) => {
        ::metrics::counter!($key $(, $tag_key => $tag_val.to_string())*)
            .increment($crate::metrics::macros::MetricValue::to_f64($value) as u64)
    };
}

#[macro_export]
macro_rules! gauge {
    ($key:expr, $value:expr $(, $tag_key:expr => $tag_val:expr)* $(,)?) => {
        ::metrics::gauge!($key $(, $tag_key => $tag_val.to_string())*)
            .set($crate::metrics::macros::MetricValue::to_f64($value))
    };
}

#[macro_export]
macro_rules! timer {
    ($key:expr, $value:expr $(, $tag_key:expr => $tag_val:expr)* $(,)?) => {
        ::metrics::histogram!($key $(, $tag_key => $tag_val.to_string())*)
            .record($crate::metrics::macros::MetricValue::to_f64($value))
    };
}
