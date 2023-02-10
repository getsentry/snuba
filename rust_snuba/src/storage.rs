use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct StorageKeys {
    key: String,
    set_key: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Schema {
    local_table_name: String,
    dist_table_name: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Processor {
    name: String
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct StreamLoader {
    processor: Processor,
    default_topic: String
}


#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Storage {
    version: String,
    kind: String,
    name: String,
    schema: Schema,
    storage: StorageKeys,
    stream_loader: StreamLoader
}

impl Storage {
    // Loads storage from yaml file
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f = File::open(path)?;
        let d: Storage = serde_yaml::from_reader(f)?;
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_load_storage() {
        use super::*;

        let storage_yaml_path = "../snuba/datasets/configuration/querylog/storages/querylog.yaml";

        let storage = Storage::load(storage_yaml_path).unwrap();

        assert_eq!(storage.name, "querylog");
        assert_eq!(storage.storage.key, "querylog");
        assert_eq!(storage.storage.set_key, "querylog");
        assert_eq!(storage.schema.local_table_name, "querylog_local");
        assert_eq!(storage.schema.dist_table_name, "querylog_dist");
        assert_eq!(storage.stream_loader.default_topic, "snuba-queries");
    }
}
