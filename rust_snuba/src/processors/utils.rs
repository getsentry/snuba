use chrono::{DateTime, NaiveDateTime, Utc};
use schemars::JsonSchema;
use sentry_options::options;
use sentry_protos::snuba::v1::TraceItemType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One week in seconds. The eap_items table is partitioned by
/// `(retention_days, toMonday(timestamp))`, so any event more than a week
/// in the future will add more parts.
pub const INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS: i64 = 7 * 24 * 60 * 60;

/// sentry-options key. When `true`, the eap-items consumer skips messages
/// whose event `timestamp` is more than one week in the future or older than
/// the item's retention (see `out_of_valid_interval_secs`).
pub const DROP_INVALID_TIMESTAMPS_KEY: &str = "eap_items_drop_invalid_timestamps";

// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
// Notice the differennce of .%fZ vs %.fZ, this comes from a difference in how rust's chrono handles the format
const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

/// Returns true if `ts` is more than one week after `now` or more than
/// `retention_days` before `now`. Items past retention would be TTL-deleted on
/// arrival, so there is nothing to keep.
pub fn out_of_valid_interval_secs(
    ts: DateTime<Utc>,
    now: DateTime<Utc>,
    retention_days: u16,
) -> bool {
    if ts < now - chrono::Duration::days(retention_days as i64) {
        return true;
    }
    ts > now + chrono::Duration::seconds(INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS)
}

pub fn get_drop_invalid_timestamps_enabled() -> bool {
    options("snuba")
        .ok()
        .and_then(|o| o.get(DROP_INVALID_TIMESTAMPS_KEY).ok())
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

#[derive(Clone, Copy)]
pub enum RetentionKind {
    Standard,
    Downsampled,
}

impl RetentionKind {
    fn option_key(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Downsampled => "downsampled",
        }
    }

    fn default_default(self) -> u16 {
        match self {
            Self::Standard => 30,
            Self::Downsampled => 396,
        }
    }

    fn default_max(self) -> u16 {
        match self {
            Self::Standard => 90,
            Self::Downsampled => 396,
        }
    }
}

fn retention_option(kind: RetentionKind, field: &str, fallback: u16) -> u16 {
    options("snuba")
        .ok()
        .and_then(|o| o.get("retention_days").ok())
        .and_then(|v| v.get(kind.option_key()).cloned())
        .and_then(|entry| entry.get(field).and_then(|n| n.as_u64()))
        .and_then(|n| u16::try_from(n).ok())
        .filter(|&n| n > 0)
        .unwrap_or(fallback)
}

/// Standard retention values that may be written. Tables are partitioned by
/// `(retention_days, toMonday(timestamp))`, so every distinct value adds a full
/// set of partitions. Bucketing keeps that set bounded.
pub const SUPPORTED_RETENTION_DAYS: [u16; 6] = [7, 30, 60, 90, 180, 366];

/// Values at or above this round down to the nearest supported value.
/// Unsupported values below it are rejected rather than rounded.
pub const MIN_ROUNDED_RETENTION_DAYS: u16 = 30;

/// Preprod (Size Analysis) items are retained for 13 months. This is their own
/// supported value and cap, regardless of the `retention_days` option.
pub const PREPROD_RETENTION_DAYS: u16 = 396;

const PREPROD_SUPPORTED_RETENTION_DAYS: [u16; 7] =
    [7, 30, 60, 90, 180, 366, PREPROD_RETENTION_DAYS];

/// Cap `value` at `max` and round down to the nearest value in `supported`.
/// Returns `None` when the capped value is below `MIN_ROUNDED_RETENTION_DAYS`
/// and is not itself supported.
fn bucket_retention(value: u16, max: u16, supported: &[u16]) -> Option<u16> {
    let capped = value.min(max);
    if supported.contains(&capped) {
        return Some(capped);
    }
    if capped < MIN_ROUNDED_RETENTION_DAYS {
        return None;
    }
    supported.iter().copied().filter(|&d| d <= capped).max()
}

fn clamp_retention(value: Option<u16>, kind: RetentionKind) -> u16 {
    let Some(value) = value.filter(|&n| n > 0) else {
        return retention_option(kind, "default", kind.default_default());
    };
    value.min(retention_option(kind, "max", kind.default_max()))
}

fn standard_default() -> u16 {
    let kind = RetentionKind::Standard;
    retention_option(kind, "default", kind.default_default())
}

fn standard_max() -> u16 {
    let kind = RetentionKind::Standard;
    retention_option(kind, "max", kind.default_max())
}

/// Standard: missing/non-positive -> configured default (30). Otherwise cap at
/// the configured max (90) and round down to a supported value. `None` means
/// the value is unsupported and the message must not be ingested.
pub fn enforce_standard_retention(value: Option<u16>) -> Option<u16> {
    match value.filter(|&n| n > 0) {
        None => Some(standard_default()),
        Some(value) => bucket_retention(value, standard_max(), &SUPPORTED_RETENTION_DAYS),
    }
}

/// Like `enforce_standard_retention`, with per-item-type exceptions. Preprod
/// also supports `PREPROD_RETENTION_DAYS` and is capped there.
pub fn enforce_standard_retention_for_item_type(
    value: Option<u16>,
    item_type: TraceItemType,
) -> Option<u16> {
    if item_type != TraceItemType::Preprod {
        return enforce_standard_retention(value);
    }
    match value.filter(|&n| n > 0) {
        None => Some(standard_default()),
        Some(value) => bucket_retention(
            value,
            PREPROD_RETENTION_DAYS,
            &PREPROD_SUPPORTED_RETENTION_DAYS,
        ),
    }
}

/// Downsampled: missing/non-positive -> 396, otherwise cap at 396.
pub fn enforce_downsampled_retention(value: Option<u16>) -> u16 {
    clamp_retention(value, RetentionKind::Downsampled)
}

/// Enforce both windows for an EAP item. Standard is bucketed (see
/// `enforce_standard_retention_for_item_type`); `None` means the item must not
/// be ingested. Downsampled is at least standard; unset downsampled copies the
/// standard value.
pub fn enforce_retentions(
    retention_days: Option<u16>,
    downsampled_retention_days: Option<u16>,
    item_type: TraceItemType,
) -> Option<(u16, u16)> {
    let retention_days = enforce_standard_retention_for_item_type(retention_days, item_type)?;
    let downsampled_retention_days = match downsampled_retention_days.filter(|&n| n > 0) {
        Some(value) => enforce_downsampled_retention(Some(value)).max(retention_days),
        None => retention_days,
    };
    Some((retention_days, downsampled_retention_days))
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

/// Formats a DateTime64 tick count (at `precision` fractional digits) as a
/// quoted `"<seconds>.<fraction>"` string for JSONEachRow inserts.
///
/// Since ClickHouse 26.8, an unquoted JSON number fed to a DateTime64 column
/// is read as seconds instead of raw ticks
/// (https://github.com/ClickHouse/ClickHouse/pull/108091). A quoted decimal
/// seconds string is parsed identically by old and new versions.
pub fn format_datetime64(ticks: u64, precision: u32) -> String {
    let scale = 10u64.pow(precision);
    format!(
        "{}.{:0width$}",
        ticks / scale,
        ticks % scale,
        width = precision as usize
    )
}

pub fn serialize_datetime64_ms<S: Serializer>(ticks: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_datetime64(*ticks, 3))
}

pub fn serialize_datetime64_us<S: Serializer>(ticks: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_datetime64(*ticks, 6))
}

pub fn serialize_opt_datetime64_us<S: Serializer>(
    ticks: &Option<u64>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match ticks {
        Some(t) => serialize_datetime64_us(t, s),
        None => s.serialize_none(),
    }
}

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
    fn test_standard_defaults() {
        init_options();
        assert_eq!(enforce_standard_retention(None), Some(30));
        assert_eq!(enforce_standard_retention(Some(0)), Some(30));
        assert_eq!(enforce_standard_retention(Some(30)), Some(30));
        assert_eq!(enforce_standard_retention(Some(60)), Some(60));
        assert_eq!(enforce_standard_retention(Some(89)), Some(60));
        assert_eq!(enforce_standard_retention(Some(90)), Some(90));
        assert_eq!(enforce_standard_retention(Some(100)), Some(90));
        assert_eq!(enforce_standard_retention(Some(366)), Some(90));
    }

    #[test]
    fn test_standard_below_rounding_threshold() {
        init_options();
        // Supported values below 30 are kept; anything else below 30 is rejected.
        assert_eq!(enforce_standard_retention(Some(7)), Some(7));
        assert_eq!(enforce_standard_retention(Some(1)), None);
        assert_eq!(enforce_standard_retention(Some(8)), None);
        assert_eq!(enforce_standard_retention(Some(14)), None);
        assert_eq!(enforce_standard_retention(Some(29)), None);
    }

    #[test]
    fn test_standard_buckets_with_366_cap() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "retention_days",
            json!({
                "standard": {"default": 30, "max": 366},
                "downsampled": {"default": 396, "max": 396},
            }),
        )])
        .unwrap();

        for (value, expected) in [
            (7, Some(7)),
            (14, None),
            (30, Some(30)),
            (45, Some(30)),
            (60, Some(60)),
            (89, Some(60)),
            (90, Some(90)),
            (120, Some(90)),
            (180, Some(180)),
            (365, Some(180)),
            (366, Some(366)),
            (396, Some(366)),
            (1000, Some(366)),
        ] {
            assert_eq!(enforce_standard_retention(Some(value)), expected, "{value}");
        }
    }

    #[test]
    fn test_preprod_retention() {
        init_options();
        let preprod = TraceItemType::Preprod;
        assert_eq!(
            enforce_standard_retention_for_item_type(None, preprod),
            Some(30)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(7), preprod),
            Some(7)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(14), preprod),
            None
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(30), preprod),
            Some(30)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(90), preprod),
            Some(90)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(366), preprod),
            Some(366)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(390), preprod),
            Some(366)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(396), preprod),
            Some(396)
        );
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(1000), preprod),
            Some(396)
        );
        // Other item types keep the option cap (90 by default).
        assert_eq!(
            enforce_standard_retention_for_item_type(Some(396), TraceItemType::Span),
            Some(90)
        );
        assert_eq!(
            enforce_retentions(Some(396), None, preprod),
            Some((396, 396))
        );
        assert_eq!(
            enforce_retentions(Some(396), Some(90), preprod),
            Some((396, 396))
        );
    }

    #[test]
    fn test_format_datetime64() {
        assert_eq!(format_datetime64(1677512412223, 3), "1677512412.223");
        assert_eq!(format_datetime64(1677512412005, 3), "1677512412.005");
        assert_eq!(format_datetime64(1710805688000012, 6), "1710805688.000012");
        assert_eq!(format_datetime64(0, 3), "0.000");
    }

    #[test]
    fn test_downsampled_defaults() {
        init_options();
        assert_eq!(enforce_downsampled_retention(None), 396);
        assert_eq!(enforce_downsampled_retention(Some(365)), 365);
        assert_eq!(enforce_downsampled_retention(Some(396)), 396);
        assert_eq!(enforce_downsampled_retention(Some(420)), 396);
    }

    #[test]
    fn test_option_override() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "retention_days",
            json!({
                "standard": {"default": 60, "max": 180},
                "downsampled": {"default": 180, "max": 360},
            }),
        )])
        .unwrap();

        assert_eq!(enforce_standard_retention(None), Some(60));
        assert_eq!(enforce_standard_retention(Some(100)), Some(90));
        assert_eq!(enforce_standard_retention(Some(179)), Some(90));
        assert_eq!(enforce_standard_retention(Some(200)), Some(180));
        assert_eq!(enforce_downsampled_retention(None), 180);
        assert_eq!(enforce_downsampled_retention(Some(365)), 360);
    }

    #[test]
    fn test_retention_pair() {
        init_options();
        let span = TraceItemType::Span;
        assert_eq!(enforce_retentions(None, None, span), Some((30, 30)));
        assert_eq!(enforce_retentions(Some(90), None, span), Some((90, 90)));
        assert_eq!(enforce_retentions(Some(90), Some(0), span), Some((90, 90)));
        assert_eq!(enforce_retentions(Some(90), Some(30), span), Some((90, 90)));
        assert_eq!(
            enforce_retentions(Some(90), Some(365), span),
            Some((90, 365))
        );
        assert_eq!(
            enforce_retentions(Some(90), Some(420), span),
            Some((90, 396))
        );
        assert_eq!(
            enforce_retentions(Some(100), Some(50), span),
            Some((90, 90))
        );
        assert_eq!(enforce_retentions(Some(14), Some(396), span), None);
    }

    #[test]
    fn test_non_positive_option_falls_back_to_schema_defaults() {
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "retention_days",
            json!({
                "standard": {"default": 0, "max": 0},
                "downsampled": {"default": 0, "max": 0},
            }),
        )])
        .unwrap();

        assert_eq!(enforce_standard_retention(None), Some(30));
        assert_eq!(enforce_standard_retention(Some(100)), Some(90));
        assert_eq!(enforce_downsampled_retention(None), 396);
        assert_eq!(enforce_downsampled_retention(Some(420)), 396);
    }

    const NOW_SECS: i64 = 2_000_000_000;
    const DAY_SECS: i64 = 24 * 60 * 60;

    #[test]
    fn test_out_of_valid_interval_secs_drops_messages_older_than_retention() {
        let now = DateTime::from_timestamp(NOW_SECS, 0).unwrap();
        let ts = DateTime::from_timestamp(NOW_SECS - 30 * DAY_SECS - 1, 0).unwrap();
        assert!(out_of_valid_interval_secs(ts, now, 30));
    }

    #[test]
    fn test_out_of_valid_interval_secs_drops_messages_more_than_a_week_in_the_future() {
        let now = DateTime::from_timestamp(NOW_SECS, 0).unwrap();
        let ts =
            DateTime::from_timestamp(NOW_SECS + INVALID_TIMESTAMP_FUTURE_INTERVAL_SECONDS + 1, 0)
                .unwrap();
        assert!(out_of_valid_interval_secs(ts, now, 30));
    }

    #[test]
    fn test_out_of_valid_interval_secs_keeps_messages_at_exactly_retention_old() {
        let now = DateTime::from_timestamp(NOW_SECS, 0).unwrap();
        let ts = DateTime::from_timestamp(NOW_SECS - 30 * DAY_SECS, 0).unwrap();
        assert!(!out_of_valid_interval_secs(ts, now, 30));
    }

    #[test]
    fn test_out_of_valid_interval_secs_keeps_messages_within_a_week() {
        let now = DateTime::from_timestamp(NOW_SECS, 0).unwrap();
        let past = DateTime::from_timestamp(NOW_SECS - DAY_SECS, 0).unwrap();
        assert!(!out_of_valid_interval_secs(past, now, 30));
        let future = DateTime::from_timestamp(NOW_SECS + DAY_SECS, 0).unwrap();
        assert!(!out_of_valid_interval_secs(future, now, 30));
    }

    #[test]
    fn test_out_of_valid_interval_secs_uses_item_retention() {
        // INC-2585: an item dated 2026-04-27 arriving on 2026-10-19 is past
        // 90 day retention but still inside 396 day retention.
        let now = DateTime::parse_from_rfc3339("2026-10-19T04:08:00Z")
            .unwrap()
            .to_utc();
        let ts = DateTime::parse_from_rfc3339("2026-04-27T12:00:00Z")
            .unwrap()
            .to_utc();
        assert!(out_of_valid_interval_secs(ts, now, 90));
        assert!(!out_of_valid_interval_secs(ts, now, 396));
        // 31 days old: kept on 90 day retention, dropped on 30.
        let ts = DateTime::from_timestamp(now.timestamp() - 31 * DAY_SECS, 0).unwrap();
        assert!(!out_of_valid_interval_secs(ts, now, 90));
        assert!(out_of_valid_interval_secs(ts, now, 30));
    }

    #[test]
    fn test_drop_invalid_timestamps_option() {
        init_options();
        assert!(!get_drop_invalid_timestamps_enabled());
        let _guard =
            override_options(&[("snuba", DROP_INVALID_TIMESTAMPS_KEY, json!(true))]).unwrap();
        assert!(get_drop_invalid_timestamps_enabled());
    }
}
