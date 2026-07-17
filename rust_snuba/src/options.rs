use sentry_options::options;

pub struct LoadBalancingConfig {
    pub load_balancing: String,
    pub first_offset: Option<String>,
}

pub fn get_load_balancing_config(storage_name: &str) -> LoadBalancingConfig {
    // Both keys live in the `snuba` sentry-options namespace as dicts keyed by
    // storage name (migrated from the per-storage runtime config keys
    // `clickhouse_load_balancing[_first_offset]:<storage>`).
    let snuba_options = options("snuba").ok();

    let load_balancing = snuba_options
        .as_ref()
        .and_then(|o| o.get("clickhouse_load_balancing").ok())
        .and_then(|v| {
            v.get(storage_name)
                .and_then(|s| s.as_str())
                .map(String::from)
        })
        .unwrap_or_else(|| "in_order".to_string());

    let first_offset = snuba_options
        .as_ref()
        .and_then(|o| o.get("clickhouse_load_balancing_first_offset").ok())
        .and_then(|v| {
            v.get(storage_name)
                .and_then(|s| s.as_str())
                .map(String::from)
        });

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
    options("snuba")
        .ok()
        .and_then(|o| o.get("clickhouse_max_insert_block_size").ok())
        .and_then(|v| v.get(storage_name).and_then(|n| n.as_u64()))
        .filter(|&n| n >= CLICKHOUSE_DEFAULT_MAX_INSERT_BLOCK_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentry_options::testing::override_options;
    use serde_json::json;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn init_options() {
        INIT.call_once(|| crate::init_sentry_options().unwrap());
    }

    #[test]
    fn test_load_balancing_config_defaults() {
        init_options();
        let config = get_load_balancing_config("lb_defaults_test");
        assert_eq!(config.load_balancing, "in_order");
        assert_eq!(config.first_offset, None);
    }

    #[test]
    fn test_load_balancing_config_overrides() {
        init_options();
        let _guard = override_options(&[
            (
                "snuba",
                "clickhouse_load_balancing",
                json!({ "lb_overrides_test": "first_or_random" }),
            ),
            (
                "snuba",
                "clickhouse_load_balancing_first_offset",
                json!({ "lb_overrides_test": "1" }),
            ),
        ])
        .unwrap();

        let config = get_load_balancing_config("lb_overrides_test");
        assert_eq!(config.load_balancing, "first_or_random");
        assert_eq!(config.first_offset, Some("1".to_string()));
    }
}
