use anyhow::Error;
use parking_lot::RwLock;
use pyo3::prelude::{PyModule, Python};
use std::collections::BTreeMap;
use std::time::Duration;

use rust_arroyo::timer;
use rust_arroyo::utils::timing::Deadline;

static CONFIG: RwLock<BTreeMap<String, (Option<String>, Deadline)>> = RwLock::new(BTreeMap::new());

/// Runtime config is cached for 10 seconds
pub fn get_str_config(key: &str) -> Result<Option<String>, Error> {
    let deadline = Deadline::new(Duration::from_secs(10));

    if let Some(value) = CONFIG.read().get(key) {
        let (config, deadline) = value;
        if !deadline.has_elapsed() {
            return Ok(config.clone());
        }
    }

    let rv = Python::with_gil(|py| {
        let snuba_state = PyModule::import(py, "snuba.state")?;
        let config = snuba_state
            .getattr("get_str_config")?
            .call1((key,))?
            .extract::<Option<String>>()?;

        CONFIG
            .write()
            .insert(key.to_string(), (config.clone(), deadline));
        Ok(CONFIG.read().get(key).unwrap().0.clone())
    });

    timer!("runtime_config.get_str_config", deadline.elapsed());

    rv
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_runtime_config() {
        crate::testutils::initialize_python();
        let config = get_str_config("key0");
        assert_eq!(config.unwrap(), None);
        CONFIG.write().clear();
    }

    #[test]
    fn test_runtime_config_is_stored_in_cache() {
        crate::testutils::initialize_python();
        assert!(!CONFIG.read().contains_key("key1"));
        let config = get_str_config("key1");
        assert_eq!(config.unwrap(), None);
        assert!(CONFIG.read().contains_key("key1"));
        CONFIG.write().clear();
    }

    #[test]
    fn test_runtime_config_retrieves_from_cache_within_deadline() {
        {
            CONFIG.write().insert(
                "key2".to_string(),
                (
                    Some("value2".to_string()),
                    Deadline::new(Duration::from_secs(10)),
                ),
            );
        }
        let config = get_str_config("key2");
        assert_eq!(config.unwrap(), Some("value2".to_string()));
        CONFIG.write().clear();
    }

    #[test]
    fn test_runtime_config_invalidates_cache_outside_deadline() {
        {
            CONFIG.write().insert(
                "key3".to_string(),
                (
                    Some("value3".to_string()),
                    Deadline::new(Duration::from_secs(0)),
                ),
            );
        }
        let config = get_str_config("key3");
        assert_eq!(config.unwrap(), None);
        CONFIG.write().clear();
    }
}
