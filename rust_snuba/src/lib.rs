mod config;
mod consumer;
mod strategies;
mod types;

use pyo3::prelude::*;

fn rust_snuba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // let DSN
    let sentry_dsn = Python::with_gil(|py| {
        let snuba_settings = PyModule::import(py, "snuba.settings")?;
        let dsn: Result<String, PyErr> = snuba_settings
            .getattr("SENTRY_DSN")?
            .extract();
        dsn
    });

    match sentry_dsn {
        Ok(dsn) => {
            let _guard = sentry::init((dsn, sentry::ClientOptions {
                release: sentry::release_name!(),
                ..Default::default()
            }));
        }
        Err(_) => {
            println!("Sentry DSN not found");
        },
    }



    m.add_function(wrap_pyfunction!(consumer::consumer, m)?)?;
    Ok(())
}
