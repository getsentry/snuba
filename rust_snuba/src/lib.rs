mod config;
mod consumer;
mod metrics;
mod processors;
mod runtime_config;
mod strategies;
mod types;
mod logging;

use pyo3::prelude::*;

#[pymodule]
fn rust_snuba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(consumer::consumer, m)?)?;
    m.add_function(wrap_pyfunction!(consumer::process_message, m)?)?;
    Ok(())
}
