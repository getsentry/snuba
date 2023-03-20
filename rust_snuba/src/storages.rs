use std::collections::BTreeMap;
use std::fs::File;

use crate::settings::Settings;

use anyhow::{bail, Context, Error};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(tag = "kind")]
pub enum StorageConfig {
    #[serde(rename = "readable_storage")]
    Readable {},
    #[serde(rename = "cdc_storage")]
    Cdc {},
    #[serde(rename = "writable_storage")]
    Writable(WritableStorageConfig),
}

#[derive(Deserialize)]
pub struct WritableStorageConfig {
    pub name: String,
    pub stream_loader: StreamLoaderConfig,
    pub schema: SchemaConfig,
    #[serde(default)]
    pub writer_options: WriterOptions,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct WriterOptions {
    pub input_format_skip_unknown_fields: u8,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SchemaConfig {
    pub local_table_name: String,
    pub dist_table_name: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct StreamLoaderConfig {
    pub processor: ProcessorConfig,
    pub default_topic: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ProcessorConfig {
    pub name: String,
    #[serde(default)]
    pub args: BTreeMap<String, Value>,
}

pub struct StorageRegistry {
    storages: BTreeMap<String, WritableStorageConfig>,
}

impl StorageRegistry {
    pub fn load_all(settings: &Settings) -> Result<StorageRegistry, Error> {
        let mut rv = StorageRegistry {
            storages: BTreeMap::new(),
        };

        for entry in glob::glob(&settings.storage_config_files_glob)? {
            let entry = entry?;
            let file = File::open(entry.as_path())
                .with_context(|| format!("Failed to load file {entry:?}"))?;
            let storage: StorageConfig = serde_yaml::from_reader(file)
                .with_context(|| format!("Failed to load file {entry:?}"))?;

            if let StorageConfig::Writable(writable_storage) = storage {
                if let Some(other_storage) = rv
                    .storages
                    .insert(writable_storage.name.clone(), writable_storage)
                {
                    bail!("Duplicate storage {}", other_storage.name);
                }
            }
        }

        Ok(rv)
    }

    pub fn get(&self, key: &str) -> Option<&WritableStorageConfig> {
        self.storages.get(key)
    }
}
