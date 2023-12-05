use rdkafka::config::ClientConfig as RdKafkaConfig;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct OffsetResetConfig {
    pub auto_offset_reset: String,
    pub strict_offset_reset: bool,
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    config_map: HashMap<String, String>,
    // Only applies to consumers
    offset_reset_config: Option<OffsetResetConfig>,
}

impl KafkaConfig {
    pub fn new_config(
        bootstrap_servers: Vec<String>,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut config_map = HashMap::new();
        config_map.insert("bootstrap.servers".to_string(), bootstrap_servers.join(","));
        let config = Self {
            config_map,
            offset_reset_config: None,
        };

        apply_override_params(config, override_params)
    }

    pub fn new_consumer_config(
        bootstrap_servers: Vec<String>,
        group_id: String,
        auto_offset_reset: String,
        strict_offset_reset: bool,
        max_poll_interval_ms: usize,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut config = KafkaConfig::new_config(bootstrap_servers, None);
        config.offset_reset_config = Some(OffsetResetConfig {
            auto_offset_reset,
            strict_offset_reset,
        });
        config.config_map.insert("group.id".to_string(), group_id);
        config
            .config_map
            .insert("enable.auto.commit".to_string(), "false".to_string());

        config.config_map.insert(
            "max.poll.interval.ms".to_string(),
            max_poll_interval_ms.to_string(),
        );

        // HACK: If the max poll interval is less than 45 seconds, set the session timeout
        // to the same. (its default is 45 seconds and it must be <= to max.poll.interval.ms)
        if max_poll_interval_ms < 45_000 {
            config.config_map.insert(
                "session.timeout.ms".to_string(),
                max_poll_interval_ms.to_string(),
            );
        }

        apply_override_params(config, override_params)
    }

    pub fn new_producer_config(
        bootstrap_servers: Vec<String>,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let config = KafkaConfig::new_config(bootstrap_servers, None);

        apply_override_params(config, override_params)
    }

    pub fn offset_reset_config(&self) -> Option<&OffsetResetConfig> {
        self.offset_reset_config.as_ref()
    }
}

impl From<KafkaConfig> for RdKafkaConfig {
    fn from(item: KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();
        for (key, val) in item.config_map.iter() {
            config_obj.set(key, val);
        }

        // NOTE: Offsets are explicitly managed as part of the assignment
        // callback, so preemptively resetting offsets is not enabled when
        // strict_offset_reset is enabled.
        if let Some(config) = item.offset_reset_config {
            let auto_offset_reset = match config.strict_offset_reset {
                true => "error",
                false => &config.auto_offset_reset,
            };

            config_obj.set("auto.offset.reset", auto_offset_reset);
        }

        config_obj
    }
}

fn apply_override_params(
    mut config: KafkaConfig,
    override_params: Option<HashMap<String, String>>,
) -> KafkaConfig {
    if let Some(params) = override_params {
        for (param, value) in params {
            config.config_map.insert(param, value);
        }
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
            vec!["127.0.0.1:9092".to_string()],
            "my-group".to_string(),
            "error".to_string(),
            false,
            30_000,
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
