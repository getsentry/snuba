use crate::config::EnvConfig;
use chrono::{DateTime, NaiveDateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

// Equivalent to "%Y-%m-%dT%H:%M:%S.%fZ" in python
// Notice the differennce of .%fZ vs %.fZ, this comes from a difference in how rust's chrono handles the format
pub const PAYLOAD_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

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

fn ensure_valid_datetime<'de, D>(deserializer: D) -> Result<u64, D::Error>
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
    pub u64,
);
