use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Deserializer};
use serde_json::Value;

#[derive(Clone, Default)]
pub struct ProcessorConfig {
    pub env_config: EnvConfig,
    pub storage_name: String,
    pub eap_items_emit_received_at: bool,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum BatchSizeCalculation {
    #[default]
    Rows,
    Bytes,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    pub storages: Vec<StorageConfig>,
    pub raw_topic: TopicConfig,
    pub commit_log_topic: Option<TopicConfig>,
    pub replacements_topic: Option<TopicConfig>,
    pub accepted_outcomes_topic: Option<TopicConfig>,
    pub dlq_topic: Option<TopicConfig>,
    pub accountant_topic: TopicConfig,
    pub max_batch_size: usize,
    pub max_batch_time_ms: u64,
    #[serde(default)]
    pub max_batch_size_calculation: BatchSizeCalculation,
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
    pub quantized_rebalance_consumer_group_delay_secs: Option<u64>,
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

fn default_verify() -> bool {
    true
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    pub host: String,
    pub port: u16,
    pub secure: bool,
    pub http_port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_verify")]
    pub verify: bool,
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
    pub dogstatsd_socket_path: Option<String>,
    pub default_retention_days: u16,
    pub lower_retention_days: u16,
    pub valid_retention_days: HashSet<u16>,
    pub record_cogs: bool,
    pub project_stacktrace_blacklist: Vec<u64>,
}

impl Default for EnvConfig {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            dogstatsd_socket_path: None,
            default_retention_days: 90,
            lower_retention_days: 30,
            valid_retention_days: [30, 60, 90].iter().cloned().collect(),
            record_cogs: false,
            project_stacktrace_blacklist: Vec::new(),
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

    fn parse_clickhouse_config(verify_json: &str) -> ClickhouseConfig {
        let raw = format!(
            r#"{{
                "host": "ch.example",
                "port": 9000,
                "secure": true,
                "http_port": 8443,
                "user": "default",
                "password": "secret",
                "database": "default"{verify_json}
            }}"#
        );
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn test_clickhouse_verify_defaults_to_true_when_omitted() {
        let cfg = parse_clickhouse_config("");
        assert!(cfg.verify);
        assert!(cfg.secure);
        assert_eq!(cfg.user, "default");
        assert_eq!(cfg.password, "secret");
    }

    #[test]
    fn test_clickhouse_verify_false() {
        let cfg = parse_clickhouse_config(r#", "verify": false"#);
        assert!(!cfg.verify);
    }

    #[test]
    fn test_clickhouse_verify_true() {
        let cfg = parse_clickhouse_config(r#", "verify": true"#);
        assert!(cfg.verify);
    }
}
