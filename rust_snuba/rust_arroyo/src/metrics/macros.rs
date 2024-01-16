/// Create a [`Metric`].
///
/// Instead of creating metrics directly, it is recommended to immediately record
/// metrics using the [`counter!`], [`gauge!`] or [`distribution!`] macros.
///
/// This is the recommended way to create a [`Metric`], as the
/// implementation details of it might change.
///
/// [`Metric`]: crate::metrics::Metric
#[macro_export]
macro_rules! metric {
    ($ty:ident: $key:expr, $value:expr
        $(, $($tag_key:expr => $tag_val:expr),*)?
        $(; $($tag_only_val:expr),*)?
    ) => {{
        $crate::metrics::Metric {
            key: &$key,
            ty: $crate::metrics::MetricType::$ty,

            tags: &[
                $($((Some(&$tag_key), &$tag_val),)*)?
                $($((None, &$tag_only_val),)*)?
            ],
            value: $value.into(),

            __private: (),
        }
    }};
}

/// Records a counter [`Metric`](crate::metrics::Metric) with the global [`Recorder`](crate::metrics::Recorder).
#[macro_export]
macro_rules! counter {
    ($expr:expr) => {
        $crate::__record_metric!(Counter: $expr, 1);
    };
    ($($tt:tt)+) => {
        $crate::__record_metric!(Counter: $($tt)+);
    };
}

/// Records a gauge [`Metric`](crate::metrics::Metric) with the global [`Recorder`](crate::metrics::Recorder).
#[macro_export]
macro_rules! gauge {
    ($($tt:tt)+) => {
        $crate::__record_metric!(Gauge: $($tt)+);
    };
}

/// Records a timer [`Metric`](crate::metrics::Metric) with the global [`Recorder`](crate::metrics::Recorder).
#[macro_export]
macro_rules! timer {
    ($($tt:tt)+) => {
        $crate::__record_metric!(Timer: $($tt)+);
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __record_metric {
    ($($tt:tt)+) => {{
        $crate::metrics::record_metric($crate::metric!($($tt)+));
    }};
}
