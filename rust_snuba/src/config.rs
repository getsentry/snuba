use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Deserializer};
use serde_json::Value;

#[derive(Clone, Default)]
pub struct ProcessorConfig {
    pub env_config: EnvConfig,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    pub storages: Vec<StorageConfig>,
    pub raw_topic: TopicConfig,
    pub commit_log_topic: Option<TopicConfig>,
    pub replacements_topic: Option<TopicConfig>,
    pub dlq_topic: Option<TopicConfig>,
    pub accountant_topic: TopicConfig,
    pub max_batch_size: usize,
    pub max_batch_time_ms: u64,
    pub env: EnvConfig,
}

pub fn deserialize_broker_config<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let data = RawBrokerConfig::deserialize(deserializer)?
        .iter()
        .filter_map(|(k, v)| {
            if v.is_null() {
                None
            } else if v.is_number() {
                // Numeric types are valid in confluent-kafka-python config but not in the Rust library
                Some((k.to_string(), v.as_number().unwrap().to_string()))
            } else if v.is_string() {
                if v.as_str().unwrap().is_empty() {
                    return None;
                }
                Some((k.to_string(), v.as_str().unwrap().to_string()))
            } else {
                panic!("Unsupported type");
            }
        })
        .collect();

    Ok(data)
}

#[derive(Deserialize, Debug)]
pub struct TopicConfig {
    pub physical_topic_name: String,
    pub logical_topic_name: String,
    #[serde(deserialize_with = "deserialize_broker_config")]
    pub broker_config: BrokerConfig,
}

type RawBrokerConfig = HashMap<String, Value>;

pub type BrokerConfig = HashMap<String, String>;

impl ConsumerConfig {
    pub fn load_from_str(payload: &str) -> Result<Self, anyhow::Error> {
        let d: Self = serde_json::from_str(payload)?;
        Ok(d)
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub name: String,
    pub clickhouse_table_name: String,
    pub clickhouse_cluster: ClickhouseConfig,
    pub message_processor: MessageProcessorConfig,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    pub host: String,
    pub port: u16,
    pub http_port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct MessageProcessorConfig {
    pub python_class_name: String,
    pub python_module: String,
}

#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct EnvConfig {
    pub sentry_dsn: Option<String>,
    pub dogstatsd_host: Option<String>,
    pub dogstatsd_port: Option<u16>,
    pub default_retention_days: u16,
    pub lower_retention_days: u16,
    pub valid_retention_days: HashSet<u16>,
    pub record_cogs: bool,
    pub ddm_metrics_sample_rate: f64,
}

impl Default for EnvConfig {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            dogstatsd_host: None,
            dogstatsd_port: None,
            default_retention_days: 90,
            lower_retention_days: 30,
            valid_retention_days: [30, 90].iter().cloned().collect(),
            record_cogs: false,
            ddm_metrics_sample_rate: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let raw =
            "{\"physical_topic_name\": \"test\", \"logical_topic_name\": \"test\", \"broker_config\": {\"bootstrap.servers\": \"127.0.0.1:9092\", \"queued.max.messages.kbytes\": 10000}}";

        let topic_config: TopicConfig = serde_json::from_str(raw).unwrap();

        assert_eq!(
            topic_config.broker_config["queued.max.messages.kbytes"],
            "10000"
        );
    }
}
