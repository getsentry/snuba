use anyhow::Error;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::time::Duration;

use redis::Commands;
use std::collections::HashMap;

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

pub fn get_str_config(key: &str) -> Result<Option<String>, Error> {
    let deadline = Deadline::new(Duration::from_secs(10));

    if let Some(value) = CONFIG.read().get(key) {
        let (config, deadline) = value;
        if !deadline.has_elapsed() {
            return Ok(config.clone());
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    fn set_str_config(key: &str, val: &str) -> Result<(), Error> {
        let client = get_redis_client()?;
        let mut con = client.get_connection()?;
        con.hset(CONFIG_HASHSET_KEY, key, val)?;
        Ok(())
    }

    fn del_str_config(key: &str) -> Result<(), Error> {
        let client = get_redis_client()?;
        let mut con = client.get_connection()?;
        con.hdel(CONFIG_HASHSET_KEY, key)?;
        Ok(())
    }

    #[test]
    fn test_runtime_config() {
        del_str_config("test").unwrap();
        let config = get_str_config("test");
        assert_eq!(config.unwrap(), None);
        set_str_config("test", "value").unwrap();
        assert_eq!(get_str_config("test").unwrap(), Some(String::from("value")));
        del_str_config("test").unwrap();
    }
}
