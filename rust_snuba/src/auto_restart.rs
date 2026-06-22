use std::time::Duration;

use crate::runtime_config;

/// Default interval between consumer restarts when periodic restart is enabled
/// but no explicit interval is configured: 15 minutes.
pub const DEFAULT_RESTART_INTERVAL_SECS: u64 = 900;

fn is_truthy(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true")
}

/// Returns the interval after which the consumer should gracefully shut down so
/// that it can be restarted and reconnect to Kafka, or `None` when periodic
/// restart is not enabled for `consumer_group`.
///
/// This is gated behind a feature flag and controlled by two runtime config
/// keys (read from `snuba.state`, so they can be toggled live without a
/// redeploy):
///
/// * `kafka_consumer_periodic_restart__<consumer_group>` — the feature flag.
///   Periodic restart is only enabled when this is set to a truthy value
///   (`"1"` or `"true"`). Defaults to disabled.
/// * `kafka_consumer_periodic_restart_interval_secs__<consumer_group>` — the
///   number of seconds between restarts. Must be a positive integer; defaults
///   to [`DEFAULT_RESTART_INTERVAL_SECS`] (15 minutes) when unset or invalid.
pub fn get_restart_interval(consumer_group: &str) -> Option<Duration> {
    let enabled = runtime_config::get_str_config(
        format!("kafka_consumer_periodic_restart__{consumer_group}").as_str(),
    )
    .ok()
    .flatten()
    .map(|value| is_truthy(&value))
    .unwrap_or(false);

    if !enabled {
        return None;
    }

    let interval_secs = runtime_config::get_str_config(
        format!("kafka_consumer_periodic_restart_interval_secs__{consumer_group}").as_str(),
    )
    .ok()
    .flatten()
    .and_then(|value| value.parse::<u64>().ok())
    .filter(|&secs| secs > 0)
    .unwrap_or(DEFAULT_RESTART_INTERVAL_SECS);

    Some(Duration::from_secs(interval_secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear(consumer_group: &str) {
        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart__{consumer_group}"),
            None,
        );
        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart_interval_secs__{consumer_group}"),
            None,
        );
    }

    #[test]
    fn test_disabled_by_default() {
        crate::testutils::initialize_python();
        assert_eq!(get_restart_interval("periodic_restart_default_test"), None);
    }

    #[test]
    fn test_enabled_uses_default_interval() {
        crate::testutils::initialize_python();
        let group = "periodic_restart_enabled_test";
        let _guard = scopeguard::guard((), |_| clear(group));

        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart__{group}"),
            Some("1"),
        );
        assert_eq!(
            get_restart_interval(group),
            Some(Duration::from_secs(DEFAULT_RESTART_INTERVAL_SECS))
        );
    }

    #[test]
    fn test_enabled_with_custom_interval() {
        crate::testutils::initialize_python();
        let group = "periodic_restart_custom_test";
        let _guard = scopeguard::guard((), |_| clear(group));

        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart__{group}"),
            Some("true"),
        );
        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart_interval_secs__{group}"),
            Some("60"),
        );
        assert_eq!(get_restart_interval(group), Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_invalid_interval_falls_back_to_default() {
        crate::testutils::initialize_python();
        let group = "periodic_restart_invalid_test";
        let _guard = scopeguard::guard((), |_| clear(group));

        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart__{group}"),
            Some("1"),
        );
        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart_interval_secs__{group}"),
            Some("garbage"),
        );
        assert_eq!(
            get_restart_interval(group),
            Some(Duration::from_secs(DEFAULT_RESTART_INTERVAL_SECS))
        );
    }

    #[test]
    fn test_interval_without_flag_is_ignored() {
        crate::testutils::initialize_python();
        let group = "periodic_restart_no_flag_test";
        let _guard = scopeguard::guard((), |_| clear(group));

        runtime_config::patch_str_config_for_test(
            &format!("kafka_consumer_periodic_restart_interval_secs__{group}"),
            Some("60"),
        );
        assert_eq!(get_restart_interval(group), None);
    }
}
