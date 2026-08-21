use chrono::{DateTime, NaiveDateTime, Utc};
use schemars::JsonSchema;
use sentry_options::options;
use serde::{Deserialize, Deserializer, Serialize};

// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
// Notice the differennce of .%fZ vs %.fZ, this comes from a difference in how rust's chrono handles the format
const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

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
}

fn retention_option(kind: RetentionKind, field: &str) -> u16 {
    let retention_days = options("snuba")
        .expect("sentry-options must be initialized")
        .get("retention_days")
        .expect("retention_days option must exist");
    retention_days
        .get(kind.option_key())
        .and_then(|entry| entry.get(field))
        .and_then(|n| n.as_u64())
        .and_then(|n| u16::try_from(n).ok())
        .expect("retention_days schema must declare a positive default and max")
}

fn clamp_retention(value: Option<u16>, kind: RetentionKind) -> u16 {
    let Some(value) = value.filter(|&n| n > 0) else {
        return retention_option(kind, "default");
    };
    value.min(retention_option(kind, "max"))
}

/// Standard: missing/non-positive -> 30, otherwise cap at 90.
pub fn enforce_standard_retention(value: Option<u16>) -> u16 {
    clamp_retention(value, RetentionKind::Standard)
}

/// Downsampled: missing/non-positive -> 396, otherwise cap at 396.
pub fn enforce_downsampled_retention(value: Option<u16>) -> u16 {
    clamp_retention(value, RetentionKind::Downsampled)
}

/// Clamp both windows. Downsampled is at least standard; unset downsampled
/// copies the standard value.
pub fn enforce_retentions(
    retention_days: Option<u16>,
    downsampled_retention_days: Option<u16>,
) -> (u16, u16) {
    let retention_days = enforce_standard_retention(retention_days);
    let downsampled_retention_days = match downsampled_retention_days.filter(|&n| n > 0) {
        Some(value) => enforce_downsampled_retention(Some(value)).max(retention_days),
        None => retention_days,
    };
    (retention_days, downsampled_retention_days)
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
    fn test_standard_defaults() {
        init_options();
        assert_eq!(enforce_standard_retention(None), 30);
        assert_eq!(enforce_standard_retention(Some(0)), 30);
        assert_eq!(enforce_standard_retention(Some(29)), 29);
        assert_eq!(enforce_standard_retention(Some(30)), 30);
        assert_eq!(enforce_standard_retention(Some(60)), 60);
        assert_eq!(enforce_standard_retention(Some(89)), 89);
        assert_eq!(enforce_standard_retention(Some(90)), 90);
        assert_eq!(enforce_standard_retention(Some(100)), 90);
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

        assert_eq!(enforce_standard_retention(None), 60);
        assert_eq!(enforce_standard_retention(Some(100)), 100);
        assert_eq!(enforce_standard_retention(Some(179)), 179);
        assert_eq!(enforce_standard_retention(Some(200)), 180);
        assert_eq!(enforce_downsampled_retention(None), 180);
        assert_eq!(enforce_downsampled_retention(Some(365)), 360);
    }

    #[test]
    fn test_retention_pair() {
        init_options();
        assert_eq!(enforce_retentions(None, None), (30, 30));
        assert_eq!(enforce_retentions(Some(90), None), (90, 90));
        assert_eq!(enforce_retentions(Some(90), Some(0)), (90, 90));
        assert_eq!(enforce_retentions(Some(90), Some(30)), (90, 90));
        assert_eq!(enforce_retentions(Some(90), Some(365)), (90, 365));
        assert_eq!(enforce_retentions(Some(90), Some(420)), (90, 396));
        assert_eq!(enforce_retentions(Some(100), Some(50)), (90, 90));
    }
}
