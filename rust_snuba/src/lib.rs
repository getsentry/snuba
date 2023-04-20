mod config;
mod consumer;
mod strategies;
mod types;

use pyo3::{prelude::*};

#[pymodule]
fn rust_snuba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    setup_sentry(_py);
    m.add_function(wrap_pyfunction!(consumer::consumer, m)?)?;
    m.add_function(wrap_pyfunction!(setup_sentry, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn setup_sentry(_py: Python<'_>) {
    env_logger::init();
    let sentry_dsn: Result<Option<String>, PyErr> = Python::with_gil(|py| {
        let snuba_settings = PyModule::import(py, "snuba.settings")?;
        let dsn: Result<&PyAny, PyErr> = snuba_settings
            .getattr("SENTRY_DSN");

        match dsn {
            Ok(dsn) => {
                log::info!("Sentry DSN: {:?}", dsn);
                if dsn.is_none() {
                    return Ok(None);
                }
                dsn.extract::<Option<String>>()
            }
            Err(_) => {
                log::error!("Sentry DSN not found");
                Ok(None)
            },
        }
    });

    match sentry_dsn {
        Ok(dsn) => {
            let _guard = sentry::init((dsn, sentry::ClientOptions {
                release: sentry::release_name!(),
                ..Default::default()
            }));
        }
        Err(_) => {
            log::error!("Sentry DSN not found");
        },
    }
}
