use crate::config::EnvConfig;
use crate::runtime_config::get_str_config;
use crate::types::item_type_name;
use chrono::{DateTime, NaiveDateTime, Utc};
use schemars::JsonSchema;
use sentry_arroyo::counter;
use sentry_protos::snuba::v1::TraceItemType;
use serde::{Deserialize, Deserializer, Serialize};

/// One week in seconds. The eap_items table is partitioned by
/// `(retention_days, toMonday(timestamp))`, so any event more than a week
/// in the future will add more parts.
pub const INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS: i64 = 7 * 24 * 60 * 60;

/// Thirty days in seconds. Events older than this are dropped when invalid
/// timestamp dropping is enabled.
pub const INVALID_TIMESTAMP_PAST_INTERVAL_SECONDS: i64 = 30 * 24 * 60 * 60;

/// Runtime config key. When set to `"1"`, the eap-items consumer skips messages
/// whose event `timestamp` is more than one week in the future or more than
/// thirty days in the past (see `out_of_valid_interval_secs`).
pub const DROP_INVALID_TIMESTAMPS_KEY: &str = "eap_items_drop_invalid_timestamps";

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

pub fn get_drop_invalid_timestamps_enabled() -> bool {
    get_str_config(DROP_INVALID_TIMESTAMPS_KEY)
        .ok()
        .flatten()
        .map(|s| s == "1")
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
#[derive(Debug, Clone, thiserror::Error)]
#[error("message routed to DLQ (silenced)")]
pub struct SilencedDLQMessage;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
