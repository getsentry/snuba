use crate::config::EnvConfig;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Deserializer};

// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
pub const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S.%6fZ";

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
