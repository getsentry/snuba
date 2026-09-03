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

/// Whether the consumer writing to `storage_name` should validate each message
/// against its kafka schema.
///
/// `validate_schema` is a dict in the `snuba` sentry-options namespace mapping
/// storage name to a boolean; storages with no entry default to true
/// (validation on). It is read *once*, when the consumer's strategies are
/// built, and then carried as a plain bool through the processing strategies —
/// schema validation sits on the per-message hot path, so it must not pay an
/// options lookup per message. Flipping it therefore requires a consumer
/// restart (or a rebalance, which rebuilds the strategies).
///
/// This only ever turns validation *off*, and never for a consumer running
/// with --enforce-schema: that flag implies validation, and the strategies
/// (see `make_rust_processor`) OR it back in.
pub fn validate_schema_enabled(storage_name: &str) -> bool {
    options("snuba")
        .ok()
        .and_then(|o| o.get("validate_schema").ok())
        .and_then(|v| v.get(storage_name).and_then(|b| b.as_bool()))
        .unwrap_or(true)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickhouseWriteClientTimeouts {
    pub connect: Duration,
    pub request: Duration,
    pub pool_idle: Duration,
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
/// dict. Every field falls back to its default independently; non-positive
/// values fall back too.
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

/// Retry schedule for a storage's ClickHouse writer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClickhouseWriteRetryPolicy {
    pub initial_backoff_ms: f64,
    pub max_retries: usize,
    /// Fraction of the delay to jitter by. Between 0 and 1.
    pub jitter_factor: f64,
}

impl Default for ClickhouseWriteRetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 500.0,
            max_retries: 4,
            jitter_factor: 0.2,
        }
    }
}

impl ClickhouseWriteRetryPolicy {
    pub fn backoff(&self, attempt: usize) -> Duration {
        let base_ms = self.initial_backoff_ms * 2f64.powi(attempt as i32);
        let jitter = rand::random::<f64>() * self.jitter_factor - self.jitter_factor / 2.0;
        Duration::from_millis((base_ms * (1.0 + jitter)).round() as u64)
    }
}

/// Largest accepted `max_retries`. Past ~1024 the doubling in
/// [`ClickhouseWriteRetryPolicy::backoff`] overflows to infinity and the delay
/// saturates to `u64::MAX` ms, which parks the write forever.
const MAX_CLICKHOUSE_WRITE_RETRIES: u64 = 20;

/// Retry schedule for `storage_name`, from the `clickhouse_write_retry_policy`
/// dict. Every field falls back to its default independently. Zero is
/// honoured; only negative and out-of-range values fall back.
pub fn get_clickhouse_write_retry_policy(storage_name: &str) -> ClickhouseWriteRetryPolicy {
    let defaults = ClickhouseWriteRetryPolicy::default();
    let Some(entry) = options("snuba")
        .ok()
        .and_then(|o| o.get("clickhouse_write_retry_policy").ok())
        .and_then(|v| v.get(storage_name).cloned())
    else {
        return defaults;
    };

    ClickhouseWriteRetryPolicy {
        initial_backoff_ms: entry
            .get("initial_backoff_ms")
            .and_then(|n| n.as_f64())
            .filter(|ms| ms.is_finite() && *ms >= 0.0)
            .unwrap_or(defaults.initial_backoff_ms),
        max_retries: entry
            .get("max_retries")
            .and_then(|n| n.as_u64())
            .filter(|&n| n <= MAX_CLICKHOUSE_WRITE_RETRIES)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(defaults.max_retries),
        jitter_factor: entry
            .get("jitter_factor")
            .and_then(|n| n.as_f64())
            .filter(|f| (0.0..=1.0).contains(f))
            .unwrap_or(defaults.jitter_factor),
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
    fn test_validate_schema_defaults_to_enabled() {
        init_options();
        assert!(validate_schema_enabled("validate_schema_unset_test"));
    }

    #[test]
    fn test_validate_schema_can_be_disabled_per_storage() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "validate_schema",
            json!({ "validate_schema_off_test": false }),
        )])
        .unwrap();

        assert!(!validate_schema_enabled("validate_schema_off_test"));
        // Storages with no entry keep validating.
        assert!(validate_schema_enabled("validate_schema_other_storage"));
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
    fn test_write_retry_policy_default_when_unset() {
        init_options();
        assert_eq!(
            get_clickhouse_write_retry_policy("retry_unset_test"),
            ClickhouseWriteRetryPolicy::default()
        );
    }

    #[test]
    fn test_write_retry_policy_partial_override_and_zero_is_honoured() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_write_retry_policy",
            json!({ "retry_partial_test": { "max_retries": 0, "jitter_factor": 0.0 } }),
        )])
        .unwrap();

        let defaults = ClickhouseWriteRetryPolicy::default();
        let policy = get_clickhouse_write_retry_policy("retry_partial_test");

        assert_eq!(policy.max_retries, 0);
        assert_eq!(policy.jitter_factor, 0.0);
        assert_eq!(policy.initial_backoff_ms, defaults.initial_backoff_ms);

        assert_eq!(
            get_clickhouse_write_retry_policy("retry_other_storage"),
            defaults
        );
    }

    #[test]
    fn test_write_retry_policy_rejects_out_of_range() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_write_retry_policy",
            json!({
                "retry_range_test": {
                    "initial_backoff_ms": -1.0,
                    "max_retries": 10_000,
                    "jitter_factor": 5.0
                }
            }),
        )])
        .unwrap();

        assert_eq!(
            get_clickhouse_write_retry_policy("retry_range_test"),
            ClickhouseWriteRetryPolicy::default()
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

        assert_eq!(
            get_clickhouse_write_client_timeouts("timeouts_zero_test"),
            ClickhouseWriteClientTimeouts::default()
        );
    }
}
