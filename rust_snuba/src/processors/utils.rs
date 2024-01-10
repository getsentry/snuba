use crate::config::EnvConfig;
use chrono::{DateTime, NaiveDateTime, Utc};
use sentry_usage_accountant::{KafkaConfig, KafkaProducer, UsageAccountant, UsageUnit};
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

#[allow(dead_code)]
pub struct CogsAccountant {
    accountant: UsageAccountant<KafkaProducer>,
    // We only log a warning once if there was an error recording cogs. Once this is true, we no longer record.
    logged_warning: bool,
}

impl CogsAccountant {
    #[allow(dead_code)]
    fn new(bootstrap_servers: &str) -> Self {
        let config = KafkaConfig::new_producer_config(bootstrap_servers, None);
        Self {
            accountant: UsageAccountant::new_with_kafka(config, None, None),
            logged_warning: false,
        }
    }

    fn record_bytes(&mut self, resource_id: &str, app_feature: &str, amount_bytes: u64) {
        if let Err(err) =
            self.accountant
                .record(resource_id, app_feature, amount_bytes, UsageUnit::Bytes)
        {
            if !self.logged_warning {
                tracing::warn!(?err, "error recording cogs");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_cogs() {
        let mut accountant = CogsAccountant::new("127.0.0.1:9092");
        accountant.record_bytes("generic_metrics_processor_sets", "custom", 100)
    }
}
