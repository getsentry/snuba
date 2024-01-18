use pyo3::prelude::*;

pub fn initialize_python() {
    let python_executable = std::env::var("SNUBA_TEST_PYTHONEXECUTABLE").unwrap();
    let python_path = std::env::var("SNUBA_TEST_PYTHONPATH").unwrap();
    let python_path: Vec<_> = python_path.split(':').map(String::from).collect();

    Python::with_gil(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", python_executable)?;
        PyModule::import(py, "sys")?.setattr("path", python_path)?;

        // monkeypatch signal handlers in Python to noop, because otherwise python multiprocessing
        // strategies cannot be tested
        let noop_fn = py.eval("lambda *a, **kw: None", None, None).unwrap();
        PyModule::import(py, "signal")?.setattr("signal", noop_fn)?;
        Ok(())
    })
    .unwrap();
}
