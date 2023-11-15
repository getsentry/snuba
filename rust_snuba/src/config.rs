use std::collections::HashMap;

use serde::Deserialize;

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

#[derive(Deserialize, Debug)]
pub struct TopicConfig {
    pub physical_topic_name: String,
    pub logical_topic_name: String,
    pub broker_config: BrokerConfig,
}

pub type BrokerConfig = HashMap<String, Option<String>>;

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
