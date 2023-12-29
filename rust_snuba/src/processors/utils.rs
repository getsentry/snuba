use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Deserializer};

pub const DEFAULT_RETENTION_DAYS: u16 = 90;
// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
pub const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S.%6fZ";

pub fn default_retention_days() -> Option<u16> {
    Some(DEFAULT_RETENTION_DAYS)
}

pub fn hex_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let hex = String::deserialize(deserializer)?;
    u64::from_str_radix(&hex, 16).map_err(serde::de::Error::custom)
}

pub fn ensure_valid_datetime<'de, D>(deserializer: D) -> Result<u32, D::Error>
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
