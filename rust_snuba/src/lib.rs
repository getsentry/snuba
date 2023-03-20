mod consumer;
mod strategies;
mod types;
mod config;

use pyo3::prelude::*;

#[pymodule]
fn rust_snuba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(consumer::consumer, m)?)?;
    Ok(())
}
