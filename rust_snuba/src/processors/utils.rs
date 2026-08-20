use chrono::{DateTime, NaiveDateTime, Utc};
use schemars::JsonSchema;
use sentry_options::options;
use serde::{Deserialize, Deserializer, Serialize};

// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
// Notice the differennce of .%fZ vs %.fZ, this comes from a difference in how rust's chrono handles the format
const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

/// Written retention_days values are positive multiples of this quantum.
const RETENTION_QUANTUM: u16 = 30;

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

    fn default_max(self) -> u16 {
        match self {
            Self::Standard => 90,
            Self::Downsampled => 390,
        }
    }
}

fn retention_max(kind: RetentionKind) -> u16 {
    options("snuba")
        .ok()
        .and_then(|o| o.get("retention_days").ok())
        .and_then(|v| v.get(kind.option_key()).cloned())
        .and_then(|entry| entry.get("max").and_then(|n| n.as_u64()))
        .and_then(|n| u16::try_from(n).ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| kind.default_max())
}

/// Snap ``value`` to a positive multiple of 30 and clamp it to ``kind``'s max.
///
/// Missing or non-positive values become ``kind``'s max (the historical write
/// default of 90 for standard). Values below one quantum become 30.
pub fn enforce_retention(value: Option<u16>, kind: RetentionKind) -> u16 {
    let maximum = retention_max(kind);
    let Some(value) = value.filter(|&n| n > 0) else {
        return maximum;
    };
    let quantized = value.min(maximum) / RETENTION_QUANTUM * RETENTION_QUANTUM;
    if quantized == 0 {
        RETENTION_QUANTUM
    } else {
        quantized
    }
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
        assert_eq!(enforce_retention(None, RetentionKind::Standard), 90);
        assert_eq!(enforce_retention(Some(0), RetentionKind::Standard), 90);
        assert_eq!(enforce_retention(Some(29), RetentionKind::Standard), 30);
        assert_eq!(enforce_retention(Some(30), RetentionKind::Standard), 30);
        assert_eq!(enforce_retention(Some(60), RetentionKind::Standard), 60);
        assert_eq!(enforce_retention(Some(89), RetentionKind::Standard), 60);
        assert_eq!(enforce_retention(Some(90), RetentionKind::Standard), 90);
        assert_eq!(enforce_retention(Some(100), RetentionKind::Standard), 90);
    }

    #[test]
    fn test_downsampled_defaults() {
        init_options();
        assert_eq!(enforce_retention(None, RetentionKind::Downsampled), 390);
        assert_eq!(
            enforce_retention(Some(365), RetentionKind::Downsampled),
            360
        );
        assert_eq!(
            enforce_retention(Some(396), RetentionKind::Downsampled),
            390
        );
        assert_eq!(
            enforce_retention(Some(420), RetentionKind::Downsampled),
            390
        );
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

        assert_eq!(enforce_retention(None, RetentionKind::Standard), 180);
        assert_eq!(enforce_retention(Some(100), RetentionKind::Standard), 90);
        assert_eq!(enforce_retention(Some(179), RetentionKind::Standard), 150);
        assert_eq!(enforce_retention(Some(200), RetentionKind::Standard), 180);
        assert_eq!(enforce_retention(None, RetentionKind::Downsampled), 360);
        assert_eq!(
            enforce_retention(Some(365), RetentionKind::Downsampled),
            360
        );
    }
}
