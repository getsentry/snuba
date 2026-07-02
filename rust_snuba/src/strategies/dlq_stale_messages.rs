//! # DLQ Stale Messages
//!
//! `DlqStaleMessages` is an arroyo strategy that dead-letters any message whose
//! Kafka timestamp is older than a configurable, per-storage threshold. Fresh
//! messages are forwarded untouched to the next step in the pipeline.
//!
//! Unlike [`crate::strategies::blq_router::BLQRouter`] — which drains a
//! contiguous backlog of stale messages to a backlog-queue topic and flips back
//! to forwarding once it catches up — this strategy makes an independent
//! per-message decision. A stale message is reported to arroyo as an
//! [`InvalidMessage`], and the `StreamProcessor`'s DLQ machinery produces the
//! original raw payload to the storage's configured DLQ topic. If the storage
//! has no DLQ configured, the message is skipped (arroyo logs and moves on).
//!
//! The threshold is read at submit time from sentry-options
//! (`consumer.dlq_stale_threshold_seconds`, a dict keyed by storage name) so it
//! can be tuned per storage without a restart. A storage with no entry (or a
//! non-positive value) disables the behavior entirely, making this a
//! pass-through by default.

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message};
use sentry_options::options;

const STALE_THRESHOLD_KEY: &str = "consumer.dlq_stale_threshold_seconds";

pub struct DlqStaleMessages<Next> {
    next_step: Next,
    storage_name: String,
}

impl<Next> DlqStaleMessages<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    pub fn new(next_step: Next, storage_name: String) -> Self {
        Self {
            next_step,
            storage_name,
        }
    }

    /// Messages older than this are dead-lettered. Read per-message from
    /// sentry-options to allow runtime tuning. Returns `None` (disabled) when
    /// the storage has no entry or the configured value is non-positive.
    fn stale_threshold(&self) -> Option<TimeDelta> {
        options("snuba")
            .ok()
            .and_then(|o| o.get(STALE_THRESHOLD_KEY).ok())
            .and_then(|v| v.get(&self.storage_name).and_then(|n| n.as_i64()))
            .filter(|&s| s > 0)
            .map(TimeDelta::seconds)
    }
}

impl<Next> ProcessingStrategy<KafkaPayload> for DlqStaleMessages<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        let Some(threshold) = self.stale_threshold() else {
            return self.next_step.submit(message);
        };

        // A message without a timestamp can't be evaluated for staleness, so we
        // forward it rather than dead-lettering it.
        if let Some(msg_ts) = message.timestamp() {
            if Utc::now().signed_duration_since(msg_ts) > threshold {
                // Report the original message as invalid so the StreamProcessor
                // produces it to the configured DLQ topic. Non-broker messages
                // can't be dead-lettered (no partition/offset), so fall through.
                if let InnerMessage::BrokerMessage(ref broker_msg) = message.inner_message {
                    counter!(
                        "dlq_stale_messages.dlqed",
                        1,
                        "storage" => self.storage_name.clone()
                    );
                    return Err(SubmitError::InvalidMessage(InvalidMessage::from(
                        broker_msg,
                    )));
                }
            }
        }

        self.next_step.submit(message)
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use sentry_arroyo::processing::strategies::InvalidMessageReason;
    use sentry_arroyo::types::{Partition, Topic};
    use sentry_options::init_with_schemas;
    use sentry_options::testing::override_options;
    use serde_json::json;
    use std::sync::Once;

    static INIT: Once = Once::new();
    fn init_config() {
        INIT.call_once(|| init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap());
    }

    struct MockStrategy {
        submitted: Vec<Message<KafkaPayload>>,
    }

    impl MockStrategy {
        fn new() -> Self {
            Self { submitted: vec![] }
        }
    }

    impl ProcessingStrategy<KafkaPayload> for MockStrategy {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }

        fn submit(
            &mut self,
            message: Message<KafkaPayload>,
        ) -> Result<(), SubmitError<KafkaPayload>> {
            self.submitted.push(message);
            Ok(())
        }

        fn terminate(&mut self) {}

        fn join(
            &mut self,
            _timeout: Option<Duration>,
        ) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    fn make_message(timestamp: DateTime<Utc>) -> Message<KafkaPayload> {
        Message::new_broker_message(
            KafkaPayload::new(None, None, Some(b"test".to_vec())),
            Partition::new(Topic::new("test"), 0),
            0,
            timestamp,
        )
    }

    #[test]
    fn test_passthrough_when_unconfigured() {
        // No entry for this storage -> disabled, everything forwards.
        init_config();
        let mut strategy = DlqStaleMessages::new(MockStrategy::new(), "some_storage".to_string());

        strategy
            .submit(make_message(Utc::now() - TimeDelta::hours(1)))
            .unwrap();
        strategy.submit(make_message(Utc::now())).unwrap();

        assert_eq!(strategy.next_step.submitted.len(), 2);
    }

    #[test]
    fn test_non_positive_threshold_disables() {
        init_config();
        let _guard = override_options(&[(
            "snuba",
            "consumer.dlq_stale_threshold_seconds",
            json!({ "dlq_disabled_test": 0 }),
        )])
        .unwrap();
        let mut strategy =
            DlqStaleMessages::new(MockStrategy::new(), "dlq_disabled_test".to_string());

        strategy
            .submit(make_message(Utc::now() - TimeDelta::hours(1)))
            .unwrap();

        assert_eq!(strategy.next_step.submitted.len(), 1);
    }

    #[test]
    fn test_stale_message_is_dlqed() {
        init_config();
        let _guard = override_options(&[(
            "snuba",
            "consumer.dlq_stale_threshold_seconds",
            json!({ "dlq_stale_test": 10 }),
        )])
        .unwrap();
        let mut strategy = DlqStaleMessages::new(MockStrategy::new(), "dlq_stale_test".to_string());

        // Fresh message is forwarded.
        strategy.submit(make_message(Utc::now())).unwrap();
        assert_eq!(strategy.next_step.submitted.len(), 1);

        // Stale message is reported as invalid (routed to DLQ by the processor).
        let err = strategy
            .submit(make_message(Utc::now() - TimeDelta::seconds(20)))
            .unwrap_err();
        match err {
            SubmitError::InvalidMessage(invalid) => {
                assert_eq!(invalid.offset, 0);
                assert_eq!(invalid.reason, InvalidMessageReason::Invalid);
            }
            _ => panic!("expected InvalidMessage"),
        }
        // Stale message was not forwarded downstream.
        assert_eq!(strategy.next_step.submitted.len(), 1);
    }

    #[test]
    fn test_only_configured_storage_is_affected() {
        // The option targets a different storage, so ours stays disabled.
        init_config();
        let _guard = override_options(&[(
            "snuba",
            "consumer.dlq_stale_threshold_seconds",
            json!({ "other_storage": 10 }),
        )])
        .unwrap();
        let mut strategy = DlqStaleMessages::new(MockStrategy::new(), "my_storage".to_string());

        strategy
            .submit(make_message(Utc::now() - TimeDelta::hours(1)))
            .unwrap();

        assert_eq!(strategy.next_step.submitted.len(), 1);
    }
}
