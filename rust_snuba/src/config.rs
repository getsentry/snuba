use std::collections::HashMap;

use serde::{Deserialize, Deserializer};

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    pub storages: Vec<StorageConfig>,
    pub raw_topic: TopicConfig,
    pub commit_log_topic: Option<TopicConfig>,
    pub replacements_topic: Option<TopicConfig>,
    pub dlq_topic: Option<TopicConfig>,
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
            let v = v.as_ref()?;
            if v.is_empty() {
                return None;
            }
            Some((k.to_owned(), v.to_owned()))
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

type RawBrokerConfig = HashMap<String, Option<String>>;

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

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct EnvConfig {
    pub sentry_dsn: Option<String>,
    pub dogstatsd_host: Option<String>,
    pub dogstatsd_port: Option<u16>,
}
