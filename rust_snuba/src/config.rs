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
    // When false, HTTPS encryption stays on but certificate-chain validation
    // and hostname verification are both disabled. This makes the connection
    // vulnerable to man-in-the-middle attacks and must only be used when the
    // server certificate cannot be made trusted (e.g. Consul SANs vs Kubernetes
    // DNS). Defaults to true so older payloads that omit the field keep the
    // previous always-verify behavior.
    #[serde(default = "default_verify")]
    pub verify: bool,
}

fn default_verify() -> bool {
    true
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

    fn base_clickhouse_json(verify: Option<bool>) -> String {
        let verify_field = match verify {
            Some(v) => format!(", \"verify\": {v}"),
            None => String::new(),
        };
        format!(
            "{{\"host\": \"ch.local\", \"port\": 9440, \"secure\": true, \"http_port\": 8443, \"user\": \"snuba\", \"password\": \"secret\", \"database\": \"default\"{verify_field}}}"
        )
    }

    #[test]
    fn test_clickhouse_config_verify_true() {
        let raw = base_clickhouse_json(Some(true));
        let cfg: ClickhouseConfig = serde_json::from_str(&raw).unwrap();
        assert!(cfg.verify);
        assert!(cfg.secure);
        assert_eq!(cfg.user, "snuba");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.database, "default");
        assert_eq!(cfg.host, "ch.local");
        assert_eq!(cfg.http_port, 8443);
    }

    #[test]
    fn test_clickhouse_config_verify_false() {
        let raw = base_clickhouse_json(Some(false));
        let cfg: ClickhouseConfig = serde_json::from_str(&raw).unwrap();
        assert!(!cfg.verify);
    }

    #[test]
    fn test_clickhouse_config_verify_absent_defaults_to_true() {
        // Backward compatibility: older payloads that omit `verify` must still
        // deserialize, defaulting to verification ON (the previous behavior of
        // the Rust consumer, which always verified with Client::new()).
        let raw = base_clickhouse_json(None);
        let cfg: ClickhouseConfig = serde_json::from_str(&raw).unwrap();
        assert!(cfg.verify);
    }

    #[test]
    fn test_storage_config_includes_verify() {
        // Ensures verify flows through the full StorageConfig (with
        // deny_unknown_fields) the way the Python side serializes it.
        let raw = r#"{
            "name": "errors",
            "clickhouse_table_name": "errors_local",
            "clickhouse_cluster": {
                "host": "ch.local",
                "port": 9440,
                "secure": true,
                "http_port": 8443,
                "user": "snuba",
                "password": "secret",
                "database": "default",
                "verify": false
            },
            "message_processor": {
                "python_class_name": "ErrorsProcessor",
                "python_module": "snuba.consumers"
            }
        }"#;
        let storage: StorageConfig = serde_json::from_str(raw).unwrap();
        assert!(!storage.clickhouse_cluster.verify);
        assert!(storage.clickhouse_cluster.secure);
    }
}
