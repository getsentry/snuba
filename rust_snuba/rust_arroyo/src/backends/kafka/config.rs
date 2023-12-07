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
    bootstrap_servers: Vec<String>,
    extra_config: HashMap<String, String>,
}

impl KafkaConfig {
    pub fn new_config(
        bootstrap_servers: Vec<String>,
        extra_config: HashMap<String, String>,
    ) -> Self {
        Self {
            bootstrap_servers,
            extra_config,
        }
    }

    pub fn new_producer_config(
        bootstrap_servers: Vec<String>,
        extra_config: HashMap<String, String>,
    ) -> Self {
        Self::new_config(bootstrap_servers, extra_config)
    }
}

impl From<KafkaConfig> for RdKafkaConfig {
    fn from(cfg: KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();

        config_obj.set(
            "bootstrap.servers".to_string(),
            cfg.bootstrap_servers.join(","),
        );

        for (key, val) in cfg.extra_config.into_iter() {
            config_obj.set(key, val);
        }

        config_obj
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub core: KafkaConfig,
    pub group_id: String,
    pub max_poll_interval_ms: usize,
    pub session_timeout_ms: Option<usize>,
    pub offset_reset: OffsetResetConfig,
}

impl KafkaConsumerConfig {
    pub fn new(
        bootstrap_servers: Vec<String>,
        group_id: String,
        auto_offset_reset: InitialOffset,
        strict_offset_reset: bool,
        max_poll_interval_ms: usize,
        extra_config: HashMap<String, String>,
    ) -> Self {
        let core = KafkaConfig::new_config(bootstrap_servers, extra_config);
        // HACK: If the max poll interval is less than 45 seconds, set the session timeout
        // to the same. (its default is 45 seconds and it must be <= to max.poll.interval.ms)
        let session_timeout_ms = if max_poll_interval_ms < 45_000 {
            Some(max_poll_interval_ms)
        } else {
            None
        };

        Self {
            core,
            group_id,
            max_poll_interval_ms,
            session_timeout_ms,
            offset_reset: OffsetResetConfig {
                auto_offset_reset,
                strict_offset_reset,
            },
        }
    }
}

impl From<KafkaConsumerConfig> for RdKafkaConfig {
    fn from(cfg: KafkaConsumerConfig) -> Self {
        let KafkaConsumerConfig {
            core,
            group_id,
            max_poll_interval_ms,
            session_timeout_ms,
            offset_reset,
        } = cfg;

        let mut result = Self::from(core);

        result.set("enable.auto.commit".to_string(), "false".to_string());
        result.set("group.id", group_id);
        result.set("max.poll.interval.ms", max_poll_interval_ms.to_string());

        if let Some(session_timeout_ms) = session_timeout_ms {
            result.set("session.timeout.ms", session_timeout_ms.to_string());
        }

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
            HashMap::from([(
                "queued.max.messages.kbytes".to_string(),
                "1000000".to_string(),
            )]),
        );

        let rdkafka_config: RdKafkaConfig = config.into();
        assert_eq!(
            rdkafka_config.get("queued.max.messages.kbytes"),
            Some("1000000")
        );
    }
}
