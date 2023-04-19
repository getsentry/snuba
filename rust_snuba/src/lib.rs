mod config;
mod consumer;
mod strategies;
mod types;

use pyo3::prelude::*;

#[pymodule]
fn rust_snuba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // let DSN
    let sentry_dsn = Python::with_gil(|py| {
        let snuba_settings = PyModule::import(py, "snuba.settings")?;
        let dsn: Result<String, PyErr> = snuba_settings
            .getattr("SENTRY_DSN")?
            .extract();
        dsn
    }).unwrap();

    let _guard = sentry::init((sentry_dsn, sentry::ClientOptions {
        release: sentry::release_name!(),
        ..Default::default()
    }));

    m.add_function(wrap_pyfunction!(consumer::consumer, m)?)?;
    Ok(())
}
