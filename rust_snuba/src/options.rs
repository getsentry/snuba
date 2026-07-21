use std::time::Duration;

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

/// ClickHouse insert timeout for `storage_name`, from the
/// `clickhouse_insert_timeout_ms:<storage>` sentry-option (a dict in the `snuba`
/// namespace keyed by storage name). Applied to the inserter's send + end
/// timeouts:
///   - unset   → `Some(default)` (the caller's fallback)
///   - `0`     → `None` (timeout disabled)
///   - `n > 0` → `Some(Duration::from_millis(n))`
pub fn get_insert_timeout(storage_name: &str, default: Duration) -> Option<Duration> {
    match options("snuba")
        .ok()
        .and_then(|o| o.get("clickhouse_insert_timeout_ms").ok())
        .and_then(|v| v.get(storage_name).and_then(|n| n.as_u64()))
    {
        Some(0) => None,
        Some(n) => Some(Duration::from_millis(n)),
        None => Some(default),
    }
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

    #[test]
    fn test_insert_timeout_default_when_unset() {
        init_options();
        assert_eq!(
            get_insert_timeout("insert_timeout_unset_test", Duration::from_secs(2)),
            Some(Duration::from_secs(2)),
        );
    }

    #[test]
    fn test_insert_timeout_override_and_disable() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_insert_timeout_ms",
            json!({ "insert_timeout_set_test": 5000, "insert_timeout_off_test": 0 }),
        )])
        .unwrap();

        // A positive value overrides the default.
        assert_eq!(
            get_insert_timeout("insert_timeout_set_test", Duration::from_secs(2)),
            Some(Duration::from_millis(5000)),
        );
        // 0 disables the timeout, regardless of the default.
        assert_eq!(
            get_insert_timeout("insert_timeout_off_test", Duration::from_secs(2)),
            None,
        );
        // A storage absent from the dict falls back to the default.
        assert_eq!(
            get_insert_timeout("insert_timeout_absent_test", Duration::from_secs(7)),
            Some(Duration::from_secs(7)),
        );
    }
}
