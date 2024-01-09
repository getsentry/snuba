use std::sync::OnceLock;

use super::Metric;

/// The global [`Recorder`] which will receive [`Metric`] to be recorded.
pub trait Recorder {
    /// Instructs the recorder to record the given [`Metric`].
    fn record_metric(&self, metric: Metric<'_>);
}

impl<T: Recorder + ?Sized> Recorder for Box<T> {
    fn record_metric(&self, metric: Metric<'_>) {
        (**self).record_metric(metric)
    }
}

static GLOBAL_RECORDER: OnceLock<Box<dyn Recorder + Send + Sync + 'static>> = OnceLock::new();

/// Initialize the global [`Recorder`].
///
/// This will register the given `recorder` as the single global [`Recorder`] instance.
///
/// This function can only be called once, and subsequent calls will return an
/// [`Err`] in case a global [`Recorder`] has already been initialized.
pub fn init<R: Recorder + Send + Sync + 'static>(recorder: R) -> Result<(), R> {
    let mut result = Err(recorder);
    {
        let result = &mut result;
        let _ = GLOBAL_RECORDER.get_or_init(|| {
            let recorder = std::mem::replace(result, Ok(())).unwrap_err();
            Box::new(recorder)
        });
    }
    result
}

/// Records a [`Metric`] with the globally configured [`Recorder`].
///
/// This function will be a noop in case no global [`Recorder`] is configured.
pub fn record_metric(metric: Metric<'_>) {
    if let Some(recorder) = GLOBAL_RECORDER.get() {
        recorder.record_metric(metric)
    }
}
