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

/// Per-attempt deadline for a ClickHouse INSERT. Without one, a black-holed
/// connection blocks until the kernel stops retransmitting (~15 min), so the
/// retry loop never runs.
///
/// Sits just above ClickHouse's own 60s `http_receive_timeout`/`http_send_timeout`
/// so the server always answers first with something retryable, and this only
/// fires when nothing came back at all. Per attempt, not cumulative: five
/// attempts plus backoff is ~330s, under the deployed 450s max-poll-interval.
pub const DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT: Duration = Duration::from_secs(65);

/// Per-attempt INSERT deadline for `storage_name`, from the
/// `clickhouse_request_timeout_ms` dict. Applied as both `read_timeout`
/// (snapshotted at startup) and a per-request deadline (re-read each attempt,
/// so lowering it needs no redeploy). Absent or non-positive falls back to the
/// default.
pub fn get_clickhouse_request_timeout(storage_name: &str) -> Duration {
    options("snuba")
        .ok()
        .and_then(|o| o.get("clickhouse_request_timeout_ms").ok())
        .and_then(|v| v.get(storage_name).and_then(|n| n.as_u64()))
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT)
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
    fn test_clickhouse_request_timeout_default() {
        init_options();
        assert_eq!(
            get_clickhouse_request_timeout("request_timeout_defaults_test"),
            DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT
        );
    }

    #[test]
    fn test_clickhouse_request_timeout_override() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_request_timeout_ms",
            json!({ "request_timeout_overrides_test": 5_000 }),
        )])
        .unwrap();

        assert_eq!(
            get_clickhouse_request_timeout("request_timeout_overrides_test"),
            Duration::from_millis(5_000)
        );
        // A different storage keeps the default.
        assert_eq!(
            get_clickhouse_request_timeout("request_timeout_other_storage"),
            DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT
        );
    }

    #[test]
    fn test_clickhouse_request_timeout_rejects_zero() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_request_timeout_ms",
            json!({ "request_timeout_zero_test": 0 }),
        )])
        .unwrap();

        // Zero would mean no deadline, the exact behavior this prevents.
        assert_eq!(
            get_clickhouse_request_timeout("request_timeout_zero_test"),
            DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT
        );
    }
}
