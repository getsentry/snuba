use rdkafka::config::ClientConfig as RdKafkaConfig;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    config_map: HashMap<String, String>,
}

impl KafkaConfig {
    pub fn new_config(
        bootstrap_servers: Vec<String>,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut config_map = HashMap::new();
        config_map.insert("bootstrap.servers".to_string(), bootstrap_servers.join(","));

        let config = Self { config_map };

        apply_override_params(config, override_params)
    }

    pub fn new_consumer_config(
        bootstrap_servers: Vec<String>,
        group_id: String,
        auto_offset_reset: String,
        _strict_offset_reset: bool, // TODO: Implement this
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut config = KafkaConfig::new_config(bootstrap_servers, None);
        config.config_map.insert("group.id".to_string(), group_id);
        config
            .config_map
            .insert("enable.auto.commit".to_string(), "false".to_string());
        config
            .config_map
            .insert("auto.offset.reset".to_string(), auto_offset_reset);

        apply_override_params(config, override_params)
    }

    pub fn new_producer_config(
        bootstrap_servers: Vec<String>,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let config = KafkaConfig::new_config(bootstrap_servers, None);

        apply_override_params(config, override_params)
    }
}

impl From<KafkaConfig> for RdKafkaConfig {
    fn from(item: KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();
        for (key, val) in item.config_map.iter() {
            config_obj.set(key, val);
        }
        config_obj
    }
}

fn apply_override_params(
    mut config: KafkaConfig,
    override_params: Option<HashMap<String, String>>,
) -> KafkaConfig {
    match override_params {
        Some(params) => {
            for (param, value) in params {
                config.config_map.insert(param, value);
            }
        }
        None => {}
    }
    config
}

#[cfg(test)]
mod tests {
    use super::KafkaConfig;
    use rdkafka::config::ClientConfig as RdKafkaConfig;
    use std::collections::HashMap;

    #[test]
    fn test_build_consumer_configuration() {
        let config = KafkaConfig::new_consumer_config(
            vec!["localhost:9092".to_string()],
            "my-group".to_string(),
            "error".to_string(),
            false,
            Some(HashMap::from([(
                "queued.max.messages.kbytes".to_string(),
                "1000000".to_string(),
            )])),
        );

        let rdkafka_config: RdKafkaConfig = config.into();
        assert_eq!(
            rdkafka_config.get("queued.max.messages.kbytes"),
            Some("1000000")
        );
    }
}
