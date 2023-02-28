use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Cluster {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub http_port: u16,
    pub storage_sets: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Settings {
    pub clusters: Vec<Cluster>,
    pub broker_config: HashMap<String, Option<String>>,
}

impl Settings {
    pub fn load(path: &str) -> Result<Self, Box<dyn Error>> {
        let f = File::open(path)?;
        let d: Settings = serde_yaml::from_reader(f)?;
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_load_settings() {
        use super::*;

        let settings_yaml_path = "../rust_snuba/src/settings/default.yaml";

        let settings = Settings::load(settings_yaml_path).unwrap();

        let all_storage_sets: Vec<String> = [
            "cdc",
            "discover",
            "events",
            "events_ro",
            "metrics",
            "migrations",
            "outcomes",
            "querylog",
            "sessions",
            "transactions",
            "profiles",
            "functions",
            "replays",
            "generic_metrics_sets",
            "generic_metrics_distributions",
            "search_issues",
            "generic_metrics_counters",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        assert_eq!(
            settings.clusters,
            vec![Cluster {
                host: "127.0.0.1".to_string(),
                port: 9000,
                user: "default".to_string(),
                password: "".to_string(),
                http_port: 8123,
                storage_sets: all_storage_sets,
            }]
        );

        assert_eq!(
            settings.broker_config["bootstrap.servers"],
            Some("127.0.0.1:9092".to_string())
        );
    }
}
