use anyhow::Error;
use parking_lot::RwLock;
use pyo3::prelude::{PyModule, Python};
use std::collections::BTreeMap;
use std::time::Duration;

use redis;
use rust_arroyo::timer;
use rust_arroyo::utils::timing::Deadline;

static CONFIG: RwLock<BTreeMap<String, (Option<String>, Deadline)>> = RwLock::new(BTreeMap::new());

pub fn get_str_config(key: &str) -> Result<Option<String>, Error> {
    let redis_host = std::env::var("REDIS_HOST").unwrap_or(String::from("127.0.0.1"));
    let redis_port = std::env::var("REDIS_PORT").unwrap_or(String::from("6379"));
    let redis_password = std::env::var("REDIS_PASSWORD").unwrap_or(String::from(""));
    let redis_db = std::env::var("REDIS_DB").unwrap_or(String::from("1"));
    let redis_ssl = std::env::var("REDIS_SSL").unwrap_or(String::from("1"));
    let deadline = Deadline::new(Duration::from_secs(10));
    let url = format!(
        "redis://{}:{}@{}:{}/{}",
        "default", redis_password, redis_host, redis_port, redis_db
    );

    if let Some(value) = CONFIG.read().get(key) {
        let (config, deadline) = value;
        if !deadline.has_elapsed() {
            return Ok(config.clone());
        }
    }

    let client = redis::Client::open(url)?;
    let mut con = client.get_connection()?;

    let configmap: HashMap<String, String> = client.hgetall("snuba-config");
    return configmap.get(String::from(key));
}

/// Runtime config is cached for 10 seconds
pub fn get_str_config_python(key: &str) -> Result<Option<String>, Error> {
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
        let config = get_str_config("test");
        assert_eq!(config.unwrap(), None);
    }
}
