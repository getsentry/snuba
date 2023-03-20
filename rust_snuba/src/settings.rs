use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::File;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub http_port: u16,
    pub storage_sets: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Settings {
    pub clusters: Vec<Cluster>,
    pub broker_config: BrokerConfig,
    pub kafka_broker_config: HashMap<String, BrokerConfig>,
    pub config_files_path: String,
    pub storage_config_files_glob: String,
    pub kafka_topic_map: HashMap<String, String>,
}

pub type BrokerConfig = HashMap<String, Option<String>>;

impl Settings {
    pub fn load_from_json(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f = File::open(path)?;
        let d: Settings = serde_json::from_reader(f)?;
        Ok(d)
    }

    pub fn get_broker_config(&self, topic: &str) -> &BrokerConfig {
        self.kafka_broker_config.get(topic).unwrap_or(&self.broker_config)
    }
}
