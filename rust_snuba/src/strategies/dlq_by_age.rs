//! # DLQ-by-age
//!
//! A stateless arroyo strategy that sheds load during a backlog by
//! dead-lettering a configurable *proportion* of messages whose Kafka broker
//! timestamp is older than a threshold. Dropped messages are routed to the DLQ
//! via the standard `InvalidMessage` mechanism (so they can be backfilled
//! later); every other message is forwarded untouched to the next strategy.
//!
//! The point is to let a consumer that has fallen behind catch up to fresh
//! data faster: we skip a fraction of the old backlog (parking it in the DLQ)
//! while still ingesting the rest live, then backfill the DLQ separately.
//!
//! Unlike the removed `BLQRouter`, this strategy has no state machine, never
//! panics the consumer, and owns no producer. It leans entirely on arroyo's
//! DLQ buffer + policy, which are already wired up whenever the consumer has a
//! DLQ topic configured. For that reason it is only inserted into the pipeline
//! when a DLQ is configured — otherwise an `InvalidMessage` is logged and
//! silently dropped by arroyo, which would lose data.
//!
//! Dead-lettered messages use `InvalidMessageReason::Ignored` so they are not
//! reported to Sentry: age-based routing is an expected outcome, and arroyo
//! only logs `Invalid` (not `Ignored`) InvalidMessages at ERROR level, which
//! the tracing→Sentry layer captures as issues.
//!
//! ## Configuration (sentry-options, read per-message so it is runtime-tunable)
//!
//! - `consumer.dlq_by_age_sample_rate_by_storage`: dict mapping storage name to
//!   the fraction (0.0–1.0) of too-old messages to route to the DLQ. Storages
//!   with no entry default to 0.0 (disabled), which makes the strategy a
//!   no-op passthrough.
//! - `consumer.dlq_by_age_threshold_seconds`: messages whose broker timestamp
//!   is older than this are considered "too old". Defaults to 600s.

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, InvalidMessageReason, ProcessingStrategy, StrategyError,
    SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message};
use sentry_options::options;

/// Default staleness threshold when `consumer.dlq_by_age_threshold_seconds` is
/// missing or non-positive.
const DEFAULT_THRESHOLD_SECONDS: i64 = 600;

pub struct DlqByAge<Next> {
    next_step: Next,
    /// The storage this consumer writes to. Used to look up the per-storage
    /// sample rate.
    storage_name: String,
}

impl<Next> DlqByAge<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    pub fn new(next_step: Next, storage_name: String) -> Self {
        Self {
            next_step,
            storage_name,
        }
    }

    /// Fraction of too-old messages to route to the DLQ for this storage.
    /// `consumer.dlq_by_age_sample_rate_by_storage` is a dict mapping storage
    /// name to a number in [0, 1]; storages with no entry default to 0.0
    /// (disabled). Clamped to [0, 1]. Read per-message so it can be toggled
    /// without a restart.
    fn sample_rate(&self) -> f64 {
        options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.dlq_by_age_sample_rate_by_storage").ok())
            .and_then(|v| v.get(self.storage_name.as_str()).and_then(|r| r.as_f64()))
            .unwrap_or(0.0)
            .clamp(0.0, 1.0)
    }

    /// Messages older than this (by broker timestamp) are candidates for the
    /// DLQ. Read per-message from sentry-options; falls back to
    /// `DEFAULT_THRESHOLD_SECONDS` if the option is missing or non-positive.
    fn threshold(&self) -> TimeDelta {
        options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.dlq_by_age_threshold_seconds").ok())
            .and_then(|v| v.as_i64())
            .filter(|s| *s > 0)
            .map(TimeDelta::seconds)
            .unwrap_or(TimeDelta::seconds(DEFAULT_THRESHOLD_SECONDS))
    }
}

impl<Next> ProcessingStrategy<KafkaPayload> for DlqByAge<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        let rate = self.sample_rate();

        // Fast path: disabled for this storage.
        if rate <= 0.0 {
            return self.next_step.submit(message);
        }

        // Only broker messages carry a timestamp; anything else is forwarded.
        let InnerMessage::BrokerMessage(broker_message) = &message.inner_message else {
            return self.next_step.submit(message);
        };

        let elapsed = Utc::now() - broker_message.timestamp;
        if elapsed <= self.threshold() {
            return self.next_step.submit(message);
        }

        // The message is too old. Route this fraction of the backlog to the
        // DLQ; forward the rest so we keep ingesting some old data live.
        counter!("dlq_by_age.stale_seen");
        if rand::random::<f64>() < rate {
            // Use `Ignored`, not `Invalid`, as the reason. Age-based DLQ
            // routing is an expected outcome, not an error: arroyo logs an
            // `Invalid` InvalidMessage at ERROR level (which the tracing→Sentry
            // layer turns into a Sentry issue), whereas `Ignored` logs at DEBUG
            // and is not reported. Both reasons produce to the DLQ identically.
            // (Same intent as the SilencedDLQMessage path in the processor.)
            let invalid = InvalidMessage {
                partition: broker_message.partition,
                offset: broker_message.offset,
                reason: InvalidMessageReason::Ignored,
            };
            counter!("dlq_by_age.routed_to_dlq");
            Err(SubmitError::InvalidMessage(invalid))
        } else {
            self.next_step.submit(message)
        }
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
    use sentry_arroyo::types::{Partition, Topic};
    use sentry_options::testing::override_options;
    use serde_json::json;
    use std::sync::Once;

    static INIT: Once = Once::new();
    fn init_config() {
        INIT.call_once(|| crate::init_sentry_options().unwrap());
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

    /// Submit `n` messages of the given age; return the number routed to the
    /// DLQ (i.e. rejected with `InvalidMessage`). Everything else lands in the
    /// mock next_step.
    fn run_n(router: &mut DlqByAge<MockStrategy>, n: usize, age: TimeDelta) -> (usize, usize) {
        let mut dlq = 0;
        for _ in 0..n {
            match router.submit(make_message(Utc::now() - age)) {
                Ok(()) => {}
                Err(SubmitError::InvalidMessage(invalid)) => {
                    // Age-based DLQ routing must be silenced (kept out of Sentry).
                    assert_eq!(invalid.reason, InvalidMessageReason::Ignored);
                    dlq += 1;
                }
                Err(other) => panic!("unexpected submit error: {other:?}"),
            }
        }
        (dlq, router.next_step.submitted.len())
    }

    #[test]
    fn test_disabled_by_default() {
        // No option set anywhere -> sample rate defaults to 0 -> everything is
        // forwarded even when very old.
        init_config();
        let mut router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        let (dlq, forwarded) = run_n(&mut router, 20, TimeDelta::hours(5));
        assert_eq!(dlq, 0);
        assert_eq!(forwarded, 20);
    }

    #[test]
    fn test_default_threshold_is_ten_minutes() {
        init_config();
        let router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        assert_eq!(router.threshold(), TimeDelta::minutes(10));
    }

    #[test]
    fn test_rate_zero_forwards_stale() {
        // Explicit 0.0 for this storage -> passthrough.
        init_config();
        let _guard = override_options(&[
            (
                "snuba",
                "consumer.dlq_by_age_sample_rate_by_storage",
                json!({ "test_storage": 0.0 }),
            ),
            ("snuba", "consumer.dlq_by_age_threshold_seconds", json!(10)),
        ])
        .unwrap();
        let mut router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        let (dlq, forwarded) = run_n(&mut router, 20, TimeDelta::minutes(30));
        assert_eq!(dlq, 0);
        assert_eq!(forwarded, 20);
    }

    #[test]
    fn test_sample_rate_is_clamped() {
        init_config();
        let _guard = override_options(&[(
            "snuba",
            "consumer.dlq_by_age_sample_rate_by_storage",
            json!({ "test_storage": 2.0, "disabled_storage": -1.0 }),
        )])
        .unwrap();

        let enabled = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        assert_eq!(enabled.sample_rate(), 1.0);

        let disabled = DlqByAge::new(MockStrategy::new(), "disabled_storage".to_string());
        assert_eq!(disabled.sample_rate(), 0.0);
    }

    #[test]
    fn test_fresh_messages_forwarded() {
        // rate 1.0 but messages are within the threshold -> all forwarded.
        init_config();
        let _guard = override_options(&[
            (
                "snuba",
                "consumer.dlq_by_age_sample_rate_by_storage",
                json!({ "test_storage": 1.0 }),
            ),
            (
                "snuba",
                "consumer.dlq_by_age_threshold_seconds",
                json!(3600),
            ),
        ])
        .unwrap();
        let mut router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        let (dlq, forwarded) = run_n(&mut router, 20, TimeDelta::seconds(5));
        assert_eq!(dlq, 0);
        assert_eq!(forwarded, 20);
    }

    #[test]
    fn test_rate_one_routes_all_stale() {
        // rate 1.0 and messages older than the threshold -> all dead-lettered.
        init_config();
        let _guard = override_options(&[
            (
                "snuba",
                "consumer.dlq_by_age_sample_rate_by_storage",
                json!({ "test_storage": 1.0 }),
            ),
            ("snuba", "consumer.dlq_by_age_threshold_seconds", json!(10)),
        ])
        .unwrap();
        let mut router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        let (dlq, forwarded) = run_n(&mut router, 20, TimeDelta::minutes(30));
        assert_eq!(dlq, 20);
        assert_eq!(forwarded, 0);
    }

    #[test]
    fn test_per_storage_scoping() {
        // A different storage is enabled; this storage has no entry -> disabled.
        init_config();
        let _guard = override_options(&[
            (
                "snuba",
                "consumer.dlq_by_age_sample_rate_by_storage",
                json!({ "other_storage": 1.0 }),
            ),
            ("snuba", "consumer.dlq_by_age_threshold_seconds", json!(10)),
        ])
        .unwrap();
        let mut router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        let (dlq, forwarded) = run_n(&mut router, 20, TimeDelta::minutes(30));
        assert_eq!(dlq, 0);
        assert_eq!(forwarded, 20);
    }

    #[test]
    fn test_partial_rate_splits_backlog() {
        // rate 0.5 over a large backlog -> some routed, some forwarded. Bounds
        // are deliberately wide so this is not statistically flaky.
        init_config();
        let _guard = override_options(&[
            (
                "snuba",
                "consumer.dlq_by_age_sample_rate_by_storage",
                json!({ "test_storage": 0.5 }),
            ),
            ("snuba", "consumer.dlq_by_age_threshold_seconds", json!(10)),
        ])
        .unwrap();
        let mut router = DlqByAge::new(MockStrategy::new(), "test_storage".to_string());
        let (dlq, forwarded) = run_n(&mut router, 2000, TimeDelta::minutes(30));
        assert_eq!(dlq + forwarded, 2000);
        assert!(dlq > 0, "expected some messages routed to DLQ, got {dlq}");
        assert!(
            forwarded > 0,
            "expected some messages forwarded, got {forwarded}"
        );
    }
}
