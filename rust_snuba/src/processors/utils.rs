use crate::config::EnvConfig;
use crate::types::item_type_name;
use chrono::{DateTime, NaiveDateTime, Utc};
use schemars::JsonSchema;
use sentry_arroyo::counter;
use sentry_options::options;
use sentry_protos::snuba::v1::TraceItemType;
use serde::{Deserialize, Deserializer, Serialize};

/// One week in seconds. The eap_items table is partitioned by
/// `(retention_days, toMonday(timestamp))`, so any event more than a week
/// in the future will add more parts.
pub const INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS: i64 = 7 * 24 * 60 * 60;

/// Thirty days in seconds. Events older than this are dropped when invalid
/// timestamp dropping is enabled.
pub const INVALID_TIMESTAMP_PAST_INTERVAL_SECONDS: i64 = 30 * 24 * 60 * 60;

/// sentry-options key. A bool that, when `true`, makes the consumer skip
/// messages whose event `timestamp` is more than one week in the future or
/// more than thirty days in the past (see `out_of_valid_interval_secs`).
/// Applies to every storage. This is the original, global killswitch; prefer
/// the per-storage `DROP_INVALID_TIMESTAMPS_BY_STORAGE_KEY`, which is checked
/// first and only falls back to this value when it has no entry for a storage.
pub const DROP_INVALID_TIMESTAMPS_KEY: &str = "eap_items_drop_invalid_timestamps";

/// sentry-options key. A dict mapping storage name to a bool, letting each
/// storage be controlled independently (e.g. `eap_items` vs
/// `eap_items_dlq_replay`). Checked before `DROP_INVALID_TIMESTAMPS_KEY`; a
/// storage with an entry uses that value, and a storage with no entry falls
/// back to the global `DROP_INVALID_TIMESTAMPS_KEY`.
pub const DROP_INVALID_TIMESTAMPS_BY_STORAGE_KEY: &str =
    "eap_items_drop_invalid_timestamps_by_storage";

// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
// Notice the differennce of .%fZ vs %.fZ, this comes from a difference in how rust's chrono handles the format
const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

/// Returns true if `ts` is more than one week after `now` or more than thirty
/// days before `now`.
pub fn out_of_valid_interval_secs(ts: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let offset_sec = ts.signed_duration_since(now).num_seconds();
    !(-INVALID_TIMESTAMP_PAST_INTERVAL_SECONDS..=INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS)
        .contains(&offset_sec)
}

pub fn get_drop_invalid_timestamps_enabled(storage_name: &str) -> bool {
    let opts = match options("snuba") {
        Ok(opts) => opts,
        Err(_) => return false,
    };

    // Per-storage option takes precedence; only fall back to the global option
    // when this storage has no entry in the per-storage dict.
    if !storage_name.is_empty() {
        if let Some(enabled) = opts
            .get(DROP_INVALID_TIMESTAMPS_BY_STORAGE_KEY)
            .ok()
            .and_then(|v| v.get(storage_name).and_then(|b| b.as_bool()))
        {
            return enabled;
        }
    }

    opts.get(DROP_INVALID_TIMESTAMPS_KEY)
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

pub fn record_invalid_timestamp_metric(prefix: &str, is_future: bool, item_type: TraceItemType) {
    let metric_name = format!("{prefix}.dropped_out_of_range_timestamp");
    let item_type_str = item_type_name(item_type);
    counter!(metric_name, 1, "is_future" => is_future.to_string(), "item_type" => item_type_str);
}

pub fn enforce_retention(value: Option<u16>, config: &EnvConfig) -> u16 {
    let mut retention_days = value.unwrap_or(config.default_retention_days);

    if !config.valid_retention_days.contains(&retention_days) {
        if retention_days <= config.lower_retention_days {
            retention_days = config.lower_retention_days;
        } else {
            retention_days = config.default_retention_days;
        }
    }

    retention_days
}

fn ensure_valid_datetime<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let naive = NaiveDateTime::parse_from_str(&value, PAYLOAD_DATETIME_FORMAT);
    let seconds_since_epoch = match naive {
        Ok(naive_dt) => DateTime::from_naive_utc_and_offset(naive_dt, Utc),
        Err(_) => Utc::now(),
    };
    Ok(seconds_since_epoch.timestamp() as u32)
}

fn ensure_valid_datetime_64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let naive = NaiveDateTime::parse_from_str(&value, PAYLOAD_DATETIME_FORMAT);
    let milliseconds_since_epoch = match naive {
        Ok(naive_dt) => {
            let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
            dt.timestamp_millis() as u64
        }
        Err(_) => {
            let now = Utc::now();
            now.timestamp_millis() as u64
        }
    };
    Ok(milliseconds_since_epoch)
}

#[derive(Debug, Deserialize, JsonSchema, Default, Serialize)]
pub struct StringToIntDatetime(
    #[serde(deserialize_with = "ensure_valid_datetime")]
    #[schemars(with = "String")]
    pub u32,
);

#[derive(Debug, Deserialize, JsonSchema, Default, Serialize)]
pub struct StringToIntDatetime64(
    #[serde(deserialize_with = "ensure_valid_datetime_64")]
    #[schemars(with = "String")]
    pub u64,
);

/// Error type for messages that should be routed to the DLQ without being
/// reported to Sentry. The processor strategy downcasts to this type and skips
/// the usual error-level logging / Sentry capture, treating the failure as an
/// expected (silenced) DLQ outcome.
#[derive(Debug, thiserror::Error)]
#[error("message routed to DLQ (silenced)")]
pub struct SilencedDLQMessage;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
    fn test_get_drop_invalid_timestamps_enabled_per_storage_takes_precedence() {
        init_options();
        let _guard = override_options(&[
            (
                "snuba",
                DROP_INVALID_TIMESTAMPS_BY_STORAGE_KEY,
                json!({ "eap_items": true, "eap_items_dlq_replay": false }),
            ),
            // Global says false, but the per-storage entry for eap_items wins.
            ("snuba", DROP_INVALID_TIMESTAMPS_KEY, json!(false)),
        ])
        .unwrap();
        assert!(get_drop_invalid_timestamps_enabled("eap_items"));
        assert!(!get_drop_invalid_timestamps_enabled("eap_items_dlq_replay"));
    }

    #[test]
    fn test_get_drop_invalid_timestamps_enabled_falls_back_to_global() {
        init_options();
        let _guard = override_options(&[
            // Only eap_items has a per-storage entry; other storages fall back
            // to the global option.
            (
                "snuba",
                DROP_INVALID_TIMESTAMPS_BY_STORAGE_KEY,
                json!({ "eap_items": false }),
            ),
            ("snuba", DROP_INVALID_TIMESTAMPS_KEY, json!(true)),
        ])
        .unwrap();
        // Per-storage entry wins over the global true.
        assert!(!get_drop_invalid_timestamps_enabled("eap_items"));
        // No per-storage entry -> global true.
        assert!(get_drop_invalid_timestamps_enabled("eap_items_dlq_replay"));
    }

    #[test]
    fn test_get_drop_invalid_timestamps_enabled_defaults_false() {
        init_options();
        let _guard =
            override_options(&[("snuba", DROP_INVALID_TIMESTAMPS_BY_STORAGE_KEY, json!({}))])
                .unwrap();
        // Neither the per-storage dict nor the (unset) global opt it in.
        assert!(!get_drop_invalid_timestamps_enabled("eap_items"));
    }

    #[test]
    fn test_out_of_valid_interval_secs_drops_messages_more_than_thirty_days_old() {
        let now = DateTime::from_timestamp(2_000_000, 0).unwrap();
        let ts =
            DateTime::from_timestamp(2_000_000 - INVALID_TIMESTAMP_PAST_INTERVAL_SECONDS - 1, 0)
                .unwrap();
        assert!(out_of_valid_interval_secs(ts, now));
    }

    #[test]
    fn test_out_of_valid_interval_secs_drops_messages_more_than_a_week_in_the_future() {
        let now = DateTime::from_timestamp(2_000_000, 0).unwrap();
        let ts =
            DateTime::from_timestamp(2_000_000 + INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS + 1, 0)
                .unwrap();
        assert!(out_of_valid_interval_secs(ts, now));
    }

    #[test]
    fn test_out_of_valid_interval_secs_keeps_messages_at_exactly_thirty_days_old() {
        let now = DateTime::from_timestamp(2_000_000, 0).unwrap();
        let ts = DateTime::from_timestamp(2_000_000 - INVALID_TIMESTAMP_PAST_INTERVAL_SECONDS, 0)
            .unwrap();
        assert!(!out_of_valid_interval_secs(ts, now));
    }

    #[test]
    fn test_out_of_valid_interval_secs_keeps_messages_within_a_week() {
        let now = DateTime::from_timestamp(2_000_000, 0).unwrap();
        let past = DateTime::from_timestamp(2_000_000 - 86_400, 0).unwrap();
        assert!(!out_of_valid_interval_secs(past, now));
        let future = DateTime::from_timestamp(2_000_000 + 86_400, 0).unwrap();
        assert!(!out_of_valid_interval_secs(future, now));
    }
}
