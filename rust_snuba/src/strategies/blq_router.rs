//! # BLQ Router
//!
//! BLQ Router is an arroyo strategy that re-directs stale messages (by timestamp) to a configured backlog-queue topic.
//! Non-stale messages will be passively forwarded along to the next step in the arroyo strategy pipeline.

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};
use sentry_options::options;

pub struct BLQRouter<Next, ProduceStrategy> {
    next_step: Next,
    prev_flag_state: bool,
    blq_active: bool,
    producer: ProduceStrategy,

    // We have to keep this around ourself bc strategies::produce::Produce didn't define their lifetimes well
    _concurrency: Option<ConcurrencyConfig>,
    consumer_group: String,

    // Invoked from poll() when the flag flips on at runtime. Defaults to
    // std::process::exit(0) — overridden in tests so the assertion is observable.
    exit_fn: Box<dyn Fn() + Send + Sync>,
}

impl<Next> BLQRouter<Next, Produce<CommitOffsets>>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    /// next_step,
    ///     is where fresh messages get forwarded to.
    ///
    /// The stale threshold is read at runtime from sentry-options
    /// (`consumer.blq_stale_threshold_seconds`)
    /// so they can be tuned without a restart.
    pub fn new(
        next_step: Next,
        blq_producer_config: KafkaConfig,
        blq_topic: Topic,
        consumer_group: String,
    ) -> Self {
        let concurrency = ConcurrencyConfig::new(10);
        let blq_producer = Produce::new(
            CommitOffsets::new(Duration::from_millis(250)),
            KafkaProducer::new(blq_producer_config),
            &concurrency,
            TopicOrPartition::Topic(blq_topic),
        );
        let mut router = Self::new_with_strategy(next_step, blq_producer, consumer_group);
        router._concurrency = Some(concurrency);
        router
    }
}

impl<Next, ProduceStrategy> BLQRouter<Next, ProduceStrategy>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
    ProduceStrategy: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn is_enabled(consumer_group: &str) -> bool {
        options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.blq_enabled_2").ok())
            .and_then(|v| v.as_str().map(str::to_owned))
            .map(|s| !s.is_empty() && s == consumer_group)
            .unwrap_or(false)
    }

    /// Messages older than this are routed to the backlog queue.
    /// Read per-message from sentry-options to allow runtime tuning.
    /// Falls back to 30 minutes if the option is missing or non-positive.
    fn stale_threshold(&self) -> TimeDelta {
        options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.blq_stale_threshold_seconds").ok())
            .and_then(|v| v.as_i64())
            .filter(|s| *s > 0)
            .map(TimeDelta::seconds)
            .unwrap_or(TimeDelta::minutes(30))
    }

    fn new_with_strategy(
        next_step: Next,
        blq_producer: ProduceStrategy,
        consumer_group: String,
    ) -> Self {
        let flag = Self::is_enabled(&consumer_group);
        Self {
            next_step,
            prev_flag_state: flag,
            blq_active: flag,
            producer: blq_producer,
            _concurrency: None,
            consumer_group,
            exit_fn: Box::new(|| std::process::exit(0)),
        }
    }
}

impl<Next, ProduceStrategy> ProcessingStrategy<KafkaPayload> for BLQRouter<Next, ProduceStrategy>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
    ProduceStrategy: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let new_flag = Self::is_enabled(&self.consumer_group);
        if !self.prev_flag_state && new_flag {
            tracing::info!(
                "consumer.blq_enabled_2 flipped on at runtime; exiting consumer to flush downstream state"
            );
            (self.exit_fn)();
            return Ok(None);
        }
        self.prev_flag_state = new_flag;
        let produce_result = self.producer.poll();
        let next_step_result = self.next_step.poll();
        let produce_commit = produce_result?;
        let next_step_commit = next_step_result?;
        Ok(produce_commit.or(next_step_commit))
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        if !self.blq_active {
            return self.next_step.submit(message);
        }
        let msg_ts = message
            .timestamp()
            .expect("Expected kafka message to always have a timestamp, but there wasn't one");
        let elapsed = Utc::now() - msg_ts;

        let stale_threshold = self.stale_threshold();
        let is_stale = elapsed > stale_threshold;
        if !is_stale {
            self.blq_active = false;
            self.next_step.submit(message)
        } else {
            self.producer.submit(message)
        }
    }

    fn terminate(&mut self) {
        self.producer.terminate();
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let producer_result = self.producer.join(timeout);
        let next_step_result = self.next_step.join(timeout);
        if self.blq_active {
            producer_result
        } else {
            next_step_result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use sentry_arroyo::types::{Partition, Topic};
    use sentry_options::init_with_schemas;
    use sentry_options::testing::{override_options, set_override};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
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
    fn test_shutoff_when_flag_flips() {
        // Flag off: router forwards everything (fresh or stale) to next_step.
        // When the flag flips on at runtime, the next poll() invokes exit_fn.
        init_config();
        let _guard =
            override_options(&[("snuba", "consumer.blq_stale_threshold_seconds", json!(10))])
                .unwrap();

        let exited = Arc::new(AtomicBool::new(false));
        let exited_clone = exited.clone();
        let mut router = BLQRouter::new_with_strategy(
            MockStrategy::new(),
            MockStrategy::new(),
            "test_consumer_group".to_string(),
        );
        router.exit_fn = Box::new(move || exited_clone.store(true, Ordering::SeqCst));

        assert!(!router.blq_active);

        router.submit(make_message(Utc::now())).unwrap();
        router
            .submit(make_message(Utc::now() - TimeDelta::minutes(1)))
            .unwrap();
        _ = router.poll();
        assert_eq!(router.next_step.submitted.len(), 2);
        assert_eq!(router.producer.submitted.len(), 0);
        assert!(!exited.load(Ordering::SeqCst));

        set_override(
            "snuba",
            "consumer.blq_enabled_2",
            json!("test_consumer_group"),
        );

        _ = router.poll();
        assert!(exited.load(Ordering::SeqCst));
    }

    #[test]
    fn test_blq_routes_stale_then_disables_on_fresh() {
        // Flag on at boot: stale messages route to the producer; the first fresh
        // message flips blq_active off; from then on, fresh AND subsequent stale
        // both go to next_step (no re-entry without a process restart).
        init_config();
        let _guard = override_options(&[
            (
                "snuba",
                "consumer.blq_enabled_2",
                json!("test_consumer_group"),
            ),
            ("snuba", "consumer.blq_stale_threshold_seconds", json!(10)),
        ])
        .unwrap();
        let mut router = BLQRouter::new_with_strategy(
            MockStrategy::new(),
            MockStrategy::new(),
            "test_consumer_group".to_string(),
        );
        assert!(router.blq_active);

        for _ in 0..10 {
            router
                .submit(make_message(Utc::now() - TimeDelta::minutes(1)))
                .unwrap();
            _ = router.poll();
        }
        assert_eq!(router.producer.submitted.len(), 10);
        assert_eq!(router.next_step.submitted.len(), 0);
        assert!(router.blq_active);

        router.submit(make_message(Utc::now())).unwrap();
        assert!(!router.blq_active);

        router.submit(make_message(Utc::now())).unwrap();
        router
            .submit(make_message(Utc::now() - TimeDelta::minutes(1)))
            .unwrap();
        assert_eq!(router.producer.submitted.len(), 10);
        assert_eq!(router.next_step.submitted.len(), 3);
    }
}
