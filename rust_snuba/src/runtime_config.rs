use anyhow::Error;
use pyo3::prelude::{PyModule, Python};

#[allow(dead_code)]
pub fn get_str_config(key: &str) -> Result<Option<String>, Error> {
    Python::with_gil(|py| {
        let snuba_state = PyModule::import(py, "snuba.state")?;
        let config = snuba_state
            .getattr("get_str_config")?
            .call1((key,))?
            .extract::<Option<String>>()?;

        Ok(config)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_config() {
        let config = get_str_config("test");
        assert_eq!(config.unwrap(), None);
    }
}
