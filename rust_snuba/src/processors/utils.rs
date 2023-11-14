use serde::{Deserializer, Deserialize};

pub const DEFAULT_RETENTION_DAYS: u16 = 90;

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
