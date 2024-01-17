use std::collections::HashMap;

use sentry_usage_accountant::{KafkaConfig, KafkaProducer, UsageAccountant, UsageUnit};

pub type COGSResourceId = String;

pub struct CogsAccountant {
    accountant: UsageAccountant<KafkaProducer>,
    // We only log a warning once if there was an error recording cogs. Once this is true, we no longer record.
    logged_warning: bool,
}

impl CogsAccountant {
    #[allow(dead_code)]
    pub fn new(broker_config: HashMap<String, String>, topic_name: &str) -> Self {
        let config = KafkaConfig::new_producer_config(broker_config);

        Self {
            accountant: UsageAccountant::new_with_kafka(config, Some(topic_name), None),
            logged_warning: false,
        }
    }

    #[allow(dead_code)]
    pub fn record_bytes(&mut self, resource_id: &str, app_feature: &str, amount_bytes: u64) {
        if let Err(err) =
            self.accountant
                .record(resource_id, app_feature, amount_bytes, UsageUnit::Bytes)
        {
            if !self.logged_warning {
                tracing::warn!(?err, "error recording cogs");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_cogs() {
        let mut accountant = CogsAccountant::new(
            HashMap::from([(
                "bootstrap.servers".to_string(),
                "127.0.0.1:9092".to_string(),
            )]),
            "shared-resources-usage",
        );
        accountant.record_bytes("generic_metrics_processor_sets", "custom", 100)
    }
}
