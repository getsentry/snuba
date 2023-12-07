use rdkafka::config::ClientConfig as RdKafkaConfig;
use std::collections::HashMap;

use super::InitialOffset;

#[derive(Debug, Clone)]
pub struct OffsetResetConfig {
    pub auto_offset_reset: InitialOffset,
    pub strict_offset_reset: bool,
}

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
        let mut config = Self { config_map };

        config.apply_override_params(override_params);
        config
    }

    pub fn new_producer_config(
        bootstrap_servers: Vec<String>,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut config = KafkaConfig::new_config(bootstrap_servers, None);

        config.apply_override_params(override_params);
        config
    }

    fn apply_override_params(&mut self, override_params: Option<HashMap<String, String>>) {
        if let Some(params) = override_params {
            for (param, value) in params {
                self.config_map.insert(param, value);
            }
        }
    }
}

impl From<KafkaConfig> for RdKafkaConfig {
    fn from(cfg: KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();
        for (key, val) in cfg.config_map.iter() {
            config_obj.set(key, val);
        }

        config_obj
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub core: KafkaConfig,
    pub offset_reset: OffsetResetConfig,
}

impl KafkaConsumerConfig {
    pub fn new(
        bootstrap_servers: Vec<String>,
        group_id: String,
        auto_offset_reset: InitialOffset,
        strict_offset_reset: bool,
        max_poll_interval_ms: usize,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut core = KafkaConfig::new_config(bootstrap_servers, None);
        core.config_map.insert("group.id".to_string(), group_id);
        core.config_map
            .insert("enable.auto.commit".to_string(), "false".to_string());

        core.config_map.insert(
            "max.poll.interval.ms".to_string(),
            max_poll_interval_ms.to_string(),
        );

        // HACK: If the max poll interval is less than 45 seconds, set the session timeout
        // to the same. (its default is 45 seconds and it must be <= to max.poll.interval.ms)
        if max_poll_interval_ms < 45_000 {
            core.config_map.insert(
                "session.timeout.ms".to_string(),
                max_poll_interval_ms.to_string(),
            );
        }

        core.apply_override_params(override_params);

        Self {
            core,
            offset_reset: OffsetResetConfig {
                auto_offset_reset,
                strict_offset_reset,
            },
        }
    }
}

impl From<KafkaConsumerConfig> for RdKafkaConfig {
    fn from(cfg: KafkaConsumerConfig) -> Self {
        let KafkaConsumerConfig { core, offset_reset } = cfg;
        let mut result = Self::from(core);

        // NOTE: Offsets are explicitly managed as part of the assignment
        // callback, so preemptively resetting offsets is not enabled when
        // strict_offset_reset is enabled.
        let auto_offset_reset = if offset_reset.strict_offset_reset {
            InitialOffset::Error
        } else {
            offset_reset.auto_offset_reset
        };
        result.set("auto.offset.reset", auto_offset_reset.to_string());

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::backends::kafka::{config::KafkaConsumerConfig, InitialOffset};

    use rdkafka::config::ClientConfig as RdKafkaConfig;
    use std::collections::HashMap;

    #[test]
    fn test_build_consumer_configuration() {
        let config = KafkaConsumerConfig::new(
            vec!["127.0.0.1:9092".to_string()],
            "my-group".to_string(),
            InitialOffset::Error,
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
