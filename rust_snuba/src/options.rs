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

/// HTTP client timeouts for a storage's ClickHouse writer.
///
/// Defaults are chosen against the deployed ClickHouse and its fronting proxy,
/// not from first principles — see each field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickhouseWriteClientTimeouts {
    /// Connect is an intra-cluster hop; this only exists so a black-holed SYN
    /// fails fast instead of inheriting the kernel's connect backoff.
    pub connect: Duration,
    /// Per-attempt INSERT deadline. Without one, a black-holed connection
    /// blocks until the kernel stops retransmitting (~15 min) and the retry
    /// loop never runs. Sits just above ClickHouse's own 60s
    /// `http_receive_timeout`/`http_send_timeout` so the server answers first
    /// with something retryable, and this only fires when nothing came back.
    pub request: Duration,
    /// Must stay under ClickHouse's `keep_alive_timeout` (60s on the EAP
    /// clusters) and under any proxy idle timeout, or the pool hands out
    /// connections the far end already closed.
    pub pool_idle: Duration,
    /// Keepalive idle/interval/retries. Surfaces a dropped flow as a transport
    /// error in ~30s rather than leaving it to sit until `request` expires.
    /// Pinned because the host defaults (75s x 9) take 11 minutes.
    pub tcp_keepalive: Duration,
    pub tcp_keepalive_interval: Duration,
    pub tcp_keepalive_retries: u32,
}

impl Default for ClickhouseWriteClientTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            request: Duration::from_secs(65),
            pool_idle: Duration::from_secs(45),
            tcp_keepalive: Duration::from_secs(15),
            tcp_keepalive_interval: Duration::from_secs(5),
            tcp_keepalive_retries: 3,
        }
    }
}

/// Writer timeouts for `storage_name`, from the `clickhouse_write_client_timeouts`
/// dict. Every field falls back to its default independently, so an entry may
/// override only what it needs; non-positive values fall back too.
///
/// Read fresh on each call (like [`get_load_balancing_config`]). Only `request`
/// is consulted per attempt — the rest configure the shared HTTP client and so
/// take effect on restart.
pub fn get_clickhouse_write_client_timeouts(storage_name: &str) -> ClickhouseWriteClientTimeouts {
    let defaults = ClickhouseWriteClientTimeouts::default();
    let Some(entry) = options("snuba")
        .ok()
        .and_then(|o| o.get("clickhouse_write_client_timeouts").ok())
        .and_then(|v| v.get(storage_name).cloned())
    else {
        return defaults;
    };

    let positive = |field: &str| entry.get(field).and_then(|n| n.as_u64()).filter(|&n| n > 0);
    let millis =
        |field: &str, fallback: Duration| positive(field).map_or(fallback, Duration::from_millis);

    ClickhouseWriteClientTimeouts {
        connect: millis("connect_ms", defaults.connect),
        request: millis("request_ms", defaults.request),
        pool_idle: millis("pool_idle_ms", defaults.pool_idle),
        tcp_keepalive: millis("tcp_keepalive_ms", defaults.tcp_keepalive),
        tcp_keepalive_interval: millis(
            "tcp_keepalive_interval_ms",
            defaults.tcp_keepalive_interval,
        ),
        tcp_keepalive_retries: positive("tcp_keepalive_retries")
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(defaults.tcp_keepalive_retries),
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
    fn test_write_client_timeouts_default_when_unset() {
        init_options();
        assert_eq!(
            get_clickhouse_write_client_timeouts("timeouts_unset_test"),
            ClickhouseWriteClientTimeouts::default()
        );
    }

    #[test]
    fn test_write_client_timeouts_partial_override() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_write_client_timeouts",
            json!({ "timeouts_partial_test": { "request_ms": 30_000 } }),
        )])
        .unwrap();

        let defaults = ClickhouseWriteClientTimeouts::default();
        let timeouts = get_clickhouse_write_client_timeouts("timeouts_partial_test");

        // The point of the per-field fallback: setting one value must not
        // silently reset the other five.
        assert_eq!(timeouts.request, Duration::from_millis(30_000));
        assert_eq!(timeouts.connect, defaults.connect);
        assert_eq!(timeouts.pool_idle, defaults.pool_idle);
        assert_eq!(timeouts.tcp_keepalive, defaults.tcp_keepalive);
        assert_eq!(
            timeouts.tcp_keepalive_interval,
            defaults.tcp_keepalive_interval
        );
        assert_eq!(
            timeouts.tcp_keepalive_retries,
            defaults.tcp_keepalive_retries
        );

        // A different storage is unaffected.
        assert_eq!(
            get_clickhouse_write_client_timeouts("timeouts_other_storage"),
            defaults
        );
    }

    #[test]
    fn test_write_client_timeouts_full_override() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_write_client_timeouts",
            json!({
                "timeouts_full_test": {
                    "connect_ms": 1_000,
                    "request_ms": 20_000,
                    "pool_idle_ms": 25_000,
                    "tcp_keepalive_ms": 7_000,
                    "tcp_keepalive_interval_ms": 2_000,
                    "tcp_keepalive_retries": 5
                }
            }),
        )])
        .unwrap();

        assert_eq!(
            get_clickhouse_write_client_timeouts("timeouts_full_test"),
            ClickhouseWriteClientTimeouts {
                connect: Duration::from_millis(1_000),
                request: Duration::from_millis(20_000),
                pool_idle: Duration::from_millis(25_000),
                tcp_keepalive: Duration::from_millis(7_000),
                tcp_keepalive_interval: Duration::from_millis(2_000),
                tcp_keepalive_retries: 5,
            }
        );
    }

    #[test]
    fn test_write_client_timeouts_reject_non_positive() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_write_client_timeouts",
            json!({ "timeouts_zero_test": { "request_ms": 0, "tcp_keepalive_retries": 0 } }),
        )])
        .unwrap();

        // Zero would mean no deadline / no probes, the exact behavior these
        // exist to prevent, so it falls back rather than being honoured.
        assert_eq!(
            get_clickhouse_write_client_timeouts("timeouts_zero_test"),
            ClickhouseWriteClientTimeouts::default()
        );
    }
}
