use anyhow::Error;
use parking_lot::RwLock;
use pyo3::prelude::{PyModule, Python};
use redis::Commands;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

use rust_arroyo::timer;
use rust_arroyo::utils::timing::Deadline;

static CONFIG: RwLock<BTreeMap<String, (Option<String>, Deadline)>> = RwLock::new(BTreeMap::new());

static CONFIG_HASHSET_KEY: &str = "snuba-config";

fn get_redis_client() -> Result<redis::Client, redis::RedisError> {
    let redis_host = std::env::var("REDIS_HOST").unwrap_or(String::from("127.0.0.1"));
    let redis_port = std::env::var("REDIS_PORT").unwrap_or(String::from("6379"));
    let redis_password = std::env::var("REDIS_PASSWORD").unwrap_or(String::from(""));
    let redis_db = std::env::var("REDIS_DB").unwrap_or(String::from("1"));
    // TODO: handle SSL?
    let url = format!(
        "redis://{}:{}@{}:{}/{}",
        "default", redis_password, redis_host, redis_port, redis_db
    );
    redis::Client::open(url)
}

fn get_str_config_direct(key: &str) -> Result<Option<String>, Error> {
    let deadline = Deadline::new(Duration::from_secs(10));

    let client = get_redis_client()?;
    let mut con = client.get_connection()?;

    let configmap: HashMap<String, String> = con.hgetall(CONFIG_HASHSET_KEY)?;
    let val = match configmap.get(key) {
        Some(val) => Some(val.clone()),
        None => return Ok(None),
    };

    CONFIG
        .write()
        .insert(key.to_string(), (val.clone(), deadline));
    Ok(CONFIG.read().get(key).unwrap().0.clone())
}

#[allow(dead_code)]
pub fn set_str_config_direct(key: &str, val: &str) -> Result<(), Error> {
    let client = get_redis_client()?;
    let mut con = client.get_connection()?;
    con.hset(CONFIG_HASHSET_KEY, key, val)?;
    Ok(())
}

#[allow(dead_code)]
pub fn del_str_config_direct(key: &str) -> Result<(), Error> {
    let client = get_redis_client()?;
    let mut con = client.get_connection()?;
    con.hdel(CONFIG_HASHSET_KEY, key)?;
    Ok(())
}

/// Runtime config is cached for 10 seconds
pub fn get_str_config(key: &str) -> Result<Option<String>, Error> {
    let deadline = Deadline::new(Duration::from_secs(10));

    match get_str_config_direct(key) {
        Ok(val) => return Ok(val),
        Err(error) => tracing::error!(
            "Could not get config from redis directly, falling back to python {}",
            error
        ),
    }

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
