use anyhow::Error;
use parking_lot::RwLock;
use pyo3::prelude::{PyModule, Python};
use pyo3::types::PyAnyMethods;
use std::collections::BTreeMap;
use std::time::Duration;

use sentry_arroyo::timer;
use sentry_arroyo::utils::timing::Deadline;

static CONFIG: RwLock<BTreeMap<String, (Option<String>, Deadline)>> = RwLock::new(BTreeMap::new());

#[cfg(test)]
pub fn patch_str_config_for_test(key: &str, value: Option<&str>) {
    let deadline = Deadline::new(Duration::from_secs(10));

    CONFIG
        .write()
        .insert(key.to_string(), (value.map(str::to_string), deadline));
}

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

pub struct LoadBalancingConfig {
    pub load_balancing: String,
    pub first_offset: Option<String>,
}

pub fn get_load_balancing_config(storage_name: &str) -> LoadBalancingConfig {
    let load_balancing = get_str_config(&format!("clickhouse_load_balancing:{storage_name}"))
        .ok()
        .flatten()
        .unwrap_or_else(|| "in_order".to_string());

    let first_offset = get_str_config(&format!(
        "clickhouse_load_balancing_first_offset:{storage_name}"
    ))
    .ok()
    .flatten();

    LoadBalancingConfig {
        load_balancing,
        first_offset,
    }
}

/// ClickHouse's compiled-in default for `max_insert_block_size`. We refuse to
/// apply any override below this to avoid silently shrinking blocks below what
/// the server would already produce on its own.
pub const CLICKHOUSE_DEFAULT_MAX_INSERT_BLOCK_SIZE: u64 = 1_048_449;

/// Returns Some(n) if `clickhouse_max_insert_block_size:<storage_name>` is set
/// to an integer >= ClickHouse's default (1_048_449); otherwise None. Values
/// below the default are rejected, since they wouldn't increase the block size
/// past what ClickHouse already does by default. Callers should append
/// `&max_insert_block_size=<n>` to the INSERT URL when Some.
pub fn get_max_insert_block_size(storage_name: &str) -> Option<u64> {
    get_str_config(&format!("clickhouse_max_insert_block_size:{storage_name}"))
        .ok()
        .flatten()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n >= CLICKHOUSE_DEFAULT_MAX_INSERT_BLOCK_SIZE)
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

    #[test]
    fn test_load_balancing_config_defaults() {
        crate::testutils::initialize_python();
        let config = get_load_balancing_config("lb_defaults_test");
        assert_eq!(config.load_balancing, "in_order");
        assert_eq!(config.first_offset, None);
    }

    #[test]
    fn test_load_balancing_config_overrides() {
        crate::testutils::initialize_python();
        patch_str_config_for_test(
            "clickhouse_load_balancing:lb_overrides_test",
            Some("first_or_random"),
        );
        patch_str_config_for_test(
            "clickhouse_load_balancing_first_offset:lb_overrides_test",
            Some("1"),
        );

        let config = get_load_balancing_config("lb_overrides_test");
        assert_eq!(config.load_balancing, "first_or_random");
        assert_eq!(config.first_offset, Some("1".to_string()));
    }
}
