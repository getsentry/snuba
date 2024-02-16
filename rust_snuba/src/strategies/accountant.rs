use std::collections::HashMap;

use sentry_usage_accountant::{KafkaConfig, KafkaProducer, UsageAccountant, UsageUnit};
use std::time::Duration;

use crate::types::BytesInsertBatch;
use rust_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use rust_arroyo::types::Message;

pub struct CogsAccountant {
    accountant: UsageAccountant<KafkaProducer>,
    // We only log a warning once if there was an error recording cogs. Once this is true, we no longer record.
    logged_warning: bool,
}

impl CogsAccountant {
    pub fn new(broker_config: HashMap<String, String>, topic_name: &str) -> Self {
        let config = KafkaConfig::new_producer_config(broker_config);

        Self {
            accountant: UsageAccountant::new_with_kafka(config, Some(topic_name), None),
            logged_warning: false,
        }
    }

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

pub struct RecordCogs<N> {
    next_step: N,
    resource_id: String,
    accountant: CogsAccountant,
}

impl<N> RecordCogs<N> {
    pub fn new(
        next_step: N,
        resource_id: String,
        broker_config: HashMap<String, String>,
        topic_name: &str,
    ) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let accountant = CogsAccountant::new(broker_config, topic_name);

        Self {
            next_step,
            resource_id,
            accountant,
        }
    }
}

impl<N> ProcessingStrategy<BytesInsertBatch> for RecordCogs<N>
where
    N: ProcessingStrategy<BytesInsertBatch> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch>,
    ) -> Result<(), SubmitError<BytesInsertBatch>> {
        for (app_feature, amount_bytes) in message.payload().cogs_data().data.iter() {
            self.accountant
                .record_bytes(&self.resource_id, app_feature, *amount_bytes)
        }

        self.next_step.submit(message)
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
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
