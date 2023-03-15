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
    pub broker_config: HashMap<String, Option<String>>,
    pub config_files_path: String,
    pub storage_config_files_glob: String,
}

impl Settings {
    pub fn load_from_json(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f = File::open(path)?;
        let d: Settings = serde_json::from_reader(f)?;
        Ok(d)
    }
}
