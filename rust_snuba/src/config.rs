use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Deserializer};
use serde_json::Value;

#[derive(Clone, Default)]
pub struct ProcessorConfig {
    pub env_config: EnvConfig,
    pub storage_name: String,
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

    /// Returns a JSON representation of the consumer config suitable for
    /// logging, with secrets (ClickHouse password, Sentry DSN, Kafka
    /// credentials in broker configs) redacted.
    pub fn redacted_for_logging(&self) -> Value {
        let storages: Vec<Value> = self
            .storages
            .iter()
            .map(|storage| {
                serde_json::json!({
                    "name": storage.name,
                    "clickhouse_table_name": storage.clickhouse_table_name,
                    "clickhouse_cluster": {
                        "host": storage.clickhouse_cluster.host,
                        "port": storage.clickhouse_cluster.port,
                        "http_port": storage.clickhouse_cluster.http_port,
                        "user": storage.clickhouse_cluster.user,
                        "password": REDACTED,
                        "database": storage.clickhouse_cluster.database,
                        "secure": storage.clickhouse_cluster.secure,
                    },
                    "message_processor": {
                        "python_class_name": storage.message_processor.python_class_name,
                        "python_module": storage.message_processor.python_module,
                    },
                })
            })
            .collect();

        serde_json::json!({
            "storages": storages,
            "raw_topic": redacted_topic_config(&self.raw_topic),
            "commit_log_topic": self.commit_log_topic.as_ref().map(redacted_topic_config),
            "replacements_topic": self.replacements_topic.as_ref().map(redacted_topic_config),
            "accepted_outcomes_topic": self.accepted_outcomes_topic.as_ref().map(redacted_topic_config),
            "dlq_topic": self.dlq_topic.as_ref().map(redacted_topic_config),
            "accountant_topic": redacted_topic_config(&self.accountant_topic),
            "max_batch_size": self.max_batch_size,
            "max_batch_time_ms": self.max_batch_time_ms,
            "max_batch_size_calculation": format!("{:?}", self.max_batch_size_calculation),
            "env": {
                "sentry_dsn": self.env.sentry_dsn.as_ref().map(|_| REDACTED),
                "dogstatsd_host": self.env.dogstatsd_host,
                "dogstatsd_port": self.env.dogstatsd_port,
                "default_retention_days": self.env.default_retention_days,
                "lower_retention_days": self.env.lower_retention_days,
                "valid_retention_days": self.env.valid_retention_days,
                "record_cogs": self.env.record_cogs,
                "project_stacktrace_blacklist": self.env.project_stacktrace_blacklist,
            },
        })
    }
}

const REDACTED: &str = "[REDACTED]";

/// Redacts values of broker config keys that may contain credentials
/// (e.g. `sasl.password`).
fn redacted_broker_config(broker_config: &BrokerConfig) -> Value {
    let redacted = broker_config
        .iter()
        .map(|(key, value)| {
            let lower = key.to_lowercase();
            let value = if lower.contains("password") || lower.contains("secret") {
                Value::String(REDACTED.to_string())
            } else {
                Value::String(value.clone())
            };
            (key.clone(), value)
        })
        .collect();

    Value::Object(redacted)
}

fn redacted_topic_config(topic_config: &TopicConfig) -> Value {
    serde_json::json!({
        "physical_topic_name": topic_config.physical_topic_name,
        "logical_topic_name": topic_config.logical_topic_name,
        "broker_config": redacted_broker_config(&topic_config.broker_config),
        "quantized_rebalance_consumer_group_delay_secs": topic_config.quantized_rebalance_consumer_group_delay_secs,
    })
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
    pub project_stacktrace_blacklist: Vec<u64>,
}

impl Default for EnvConfig {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            dogstatsd_host: None,
            dogstatsd_port: None,
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

    #[test]
    fn test_redacted_for_logging() {
        let raw = r#"{
            "storages": [{
                "name": "errors",
                "clickhouse_table_name": "errors_local",
                "clickhouse_cluster": {
                    "host": "localhost",
                    "port": 9000,
                    "secure": false,
                    "http_port": 8123,
                    "user": "default",
                    "password": "super-secret",
                    "database": "default"
                },
                "message_processor": {
                    "python_class_name": "ErrorsProcessor",
                    "python_module": "snuba.datasets.processors.errors_processor"
                }
            }],
            "raw_topic": {
                "physical_topic_name": "events",
                "logical_topic_name": "events",
                "broker_config": {
                    "bootstrap.servers": "127.0.0.1:9092",
                    "sasl.username": "user",
                    "sasl.password": "kafka-secret"
                },
                "quantized_rebalance_consumer_group_delay_secs": null
            },
            "commit_log_topic": null,
            "replacements_topic": null,
            "accepted_outcomes_topic": null,
            "dlq_topic": null,
            "accountant_topic": {
                "physical_topic_name": "snuba-shared-resources-usage",
                "logical_topic_name": "snuba-shared-resources-usage",
                "broker_config": {},
                "quantized_rebalance_consumer_group_delay_secs": null
            },
            "max_batch_size": 1,
            "max_batch_time_ms": 1000,
            "env": {
                "sentry_dsn": "https://secret@sentry.io/1",
                "dogstatsd_host": null,
                "dogstatsd_port": null,
                "default_retention_days": 90,
                "lower_retention_days": 30,
                "valid_retention_days": [30, 60, 90],
                "record_cogs": false,
                "project_stacktrace_blacklist": []
            }
        }"#;

        let consumer_config = ConsumerConfig::load_from_str(raw).unwrap();
        let redacted = consumer_config.redacted_for_logging();
        let serialized = serde_json::to_string(&redacted).unwrap();

        // Secrets must not leak into logs.
        assert!(!serialized.contains("super-secret"));
        assert!(!serialized.contains("kafka-secret"));
        assert!(!serialized.contains("https://secret@sentry.io/1"));

        // Non-sensitive values are preserved.
        assert_eq!(
            redacted["storages"][0]["clickhouse_cluster"]["host"],
            "localhost"
        );
        assert_eq!(
            redacted["storages"][0]["clickhouse_cluster"]["password"],
            REDACTED
        );
        assert_eq!(
            redacted["raw_topic"]["broker_config"]["bootstrap.servers"],
            "127.0.0.1:9092"
        );
        assert_eq!(
            redacted["raw_topic"]["broker_config"]["sasl.username"],
            "user"
        );
        assert_eq!(
            redacted["raw_topic"]["broker_config"]["sasl.password"],
            REDACTED
        );
        assert_eq!(redacted["env"]["sentry_dsn"], REDACTED);
    }
}
