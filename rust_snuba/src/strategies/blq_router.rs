//! # BLQ Router
//!
//! BLQ Router is an arroyo strategy that re-directs stale messages (by timestamp) to a configured backlog-queue topic.
//! Non-stale messages will be passively forwarded along to the next step in the arroyo strategy pipeline.
//!
//! ## Implementation
//! its essentially a FSM
//!
//! ```text
//!                        > forward to next step
//!                              ┌fresh─┐
//!                              │      ▼
//!                           ┌──┴─────────┐
//!                   ┌fresh─►│ Forwarding ├──stale──► PANIC
//!                   │       └────────────┘
//!                   │              ▲
//!          ┌──────┐ │              │
//!     ────►│ Idle ├─┤            fresh
//!          └──────┘ │              │
//!                   │       ┌──────┴───────┐
//!                   └stale─►│ RoutingStale │
//!                           └─┬────────────┘
//!                             │        ▲
//!                             └─stale──┘
//!                          > redirect to blq
//! ```
//!
//! the reason for the panic is that there may be accumulated data downstream that needs to be flushed before we start
//! redirecting to backlog and committing those messages. The most reliable way to do this is crashing the consumer,
//! when it comes back alive the first messages it gets will be stale so it will go straight from idle to RoutingStale.

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

#[derive(Debug, PartialEq)]
enum State {
    Idle,         // no messages have gone through the router yet
    RoutingStale, // router is directing stale messages to the backlog-queue (BLQ)
    // we have processed all stale messages and are now flushing (finishing producing to BLQ)
    // when we transition to this state we will have CommitRequest for what was flushed, and poll
    // will be responsible for returning it
    Flushing(Option<CommitRequest>),
    Forwarding, // router is forwarding non-stale messages along to the next strategy
}

pub struct BLQRouter<Next, ProduceStrategy> {
    next_step: Next,
    state: State,
    producer: ProduceStrategy,

    // We have to keep this around ourself bc strategies::produce::Produce didn't define their lifetimes well
    _concurrency: Option<ConcurrencyConfig>,
}

impl<Next> BLQRouter<Next, Produce<CommitOffsets>>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    /// next_step,
    ///     is where fresh messages get forwarded to.
    ///
    /// The stale threshold and static friction are read at runtime from sentry-options
    /// (`consumer.blq_stale_threshold_seconds`, `consumer.blq_static_friction_seconds`)
    /// so they can be tuned without a restart.
    pub fn new(next_step: Next, blq_producer_config: KafkaConfig, blq_topic: Topic) -> Self {
        let concurrency = ConcurrencyConfig::new(10);
        let blq_producer = Produce::new(
            CommitOffsets::new(Duration::from_millis(250)),
            KafkaProducer::new(blq_producer_config),
            &concurrency,
            TopicOrPartition::Topic(blq_topic),
        );
        let mut router = Self::new_with_strategy(next_step, blq_producer);
        router._concurrency = Some(concurrency);
        router
    }
}

impl<Next, ProduceStrategy> BLQRouter<Next, ProduceStrategy>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
    ProduceStrategy: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn is_enabled(&self) -> bool {
        options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.blq_enabled").ok())
            .and_then(|v| v.as_bool())
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

    /// Hysteresis applied while in RoutingStale: we keep routing messages at
    /// least (stale_threshold - static_friction) old so we don't flip-flop at
    /// the boundary. A value <= 0 disables friction. Defaults to 2 minutes.
    fn static_friction(&self) -> Option<TimeDelta> {
        let secs = options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.blq_static_friction_seconds").ok())
            .and_then(|v| v.as_i64())
            .unwrap_or(120);
        if secs > 0 {
            Some(TimeDelta::seconds(secs))
        } else {
            None
        }
    }

    fn new_with_strategy(next_step: Next, blq_producer: ProduceStrategy) -> Self {
        Self {
            next_step,
            state: State::Idle,
            producer: blq_producer,
            _concurrency: None,
        }
    }
}

impl<Next, ProduceStrategy> ProcessingStrategy<KafkaPayload> for BLQRouter<Next, ProduceStrategy>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
    ProduceStrategy: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let produce_result = self.producer.poll();
        let next_step_result = self.next_step.poll();
        match &mut self.state {
            State::RoutingStale => produce_result,
            State::Forwarding | State::Idle => next_step_result,
            State::Flushing(commits) => {
                let commits = commits.take();
                self.state = State::Forwarding;
                Ok(commits)
            }
        }
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        if !self.is_enabled() {
            return self.next_step.submit(message);
        }

        let msg_ts = message
            .timestamp()
            .expect("Expected kafka message to always have a timestamp, but there wasn't one");
        let elapsed = Utc::now() - msg_ts;

        let stale_threshold = self.stale_threshold();
        let friction = self.static_friction().filter(|f| *f < stale_threshold);
        let threshold = match (&self.state, friction) {
            (State::RoutingStale, Some(f)) => stale_threshold - f,
            _ => stale_threshold,
        };
        let is_stale = elapsed > threshold;
        match (is_stale, &self.state) {
            (true, State::Forwarding) => {
                // When we transition from Forwarding to RoutingStale, there may be
                // state in memory held downstream. We crash the consumer to get rid of internal state
                // when it restarts it will have no internal state (State::Empty) and the first message in
                // the topic will be stale.
                panic!("Resetting consumer state to begin processing the stale backlog")
            }
            (true, State::Idle) | (true, State::RoutingStale) => {
                // route the stale message to the BLQ
                if self.state == State::Idle {
                    self.state = State::RoutingStale;
                }
                self.producer.submit(message)
            }
            (false, State::Idle) | (false, State::Forwarding) => {
                // Forward the fresh message along to the next step
                if self.state == State::Idle {
                    self.state = State::Forwarding;
                }
                self.next_step.submit(message)
            }
            (false, State::RoutingStale) => {
                // We hit a fresh message, so we are done routing the backlog.
                // Finish producing and committing all the state messages and
                // then switch back to forwarding fresh.

                // i know i shouldnt be blocking in submit but there was no better way to do it
                // the pipeline cant make progress until this completes anyways so it should be fine
                let flush_results = self.producer.join(Some(Duration::from_secs(5))).unwrap();
                self.state = State::Flushing(flush_results);
                Err(SubmitError::MessageRejected(
                    sentry_arroyo::processing::strategies::MessageRejected { message },
                ))
            }
            (true, State::Flushing(_)) | (false, State::Flushing(_)) => {
                Err(SubmitError::MessageRejected(
                    sentry_arroyo::processing::strategies::MessageRejected { message },
                ))
            }
        }
    }

    fn terminate(&mut self) {
        self.producer.terminate();
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let producer_result = self.producer.join(timeout);
        let next_step_result = self.next_step.join(timeout);
        match &self.state {
            State::RoutingStale => producer_result,
            State::Forwarding | State::Idle => next_step_result,
            State::Flushing(commits) => Ok(commits.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
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
        join_called: bool,
        terminate_called: bool,
    }

    impl MockStrategy {
        fn new() -> Self {
            Self {
                submitted: vec![],
                join_called: false,
                terminate_called: false,
            }
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

        fn terminate(&mut self) {
            self.terminate_called = true;
        }

        fn join(
            &mut self,
            _timeout: Option<Duration>,
        ) -> Result<Option<CommitRequest>, StrategyError> {
            self.join_called = true;
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
    #[should_panic(expected = "Resetting consumer state to begin processing the stale backlog")]
    fn test_fresh_to_stale() {
        /*
        This tests that the BLQRouter forwards business-as-usual fresh messages through it
        and crashes when it hits its first stale message
         */
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.blq_enabled", json!(true)),
            ("snuba", "consumer.blq_stale_threshold_seconds", json!(10)),
            ("snuba", "consumer.blq_static_friction_seconds", json!(0)),
        ])
        .unwrap();
        let mut router = BLQRouter::new_with_strategy(MockStrategy::new(), MockStrategy::new());
        // consuming messages as normal
        for _ in 0..10 {
            router.submit(make_message(Utc::now())).unwrap();
            _ = router.poll();
        }
        assert_eq!(router.state, State::Forwarding);
        // now theres a stale message, consumer should crash
        _ = router.submit(make_message(Utc::now() - TimeDelta::seconds(20)));
    }

    fn submit_with_retry(
        router: &mut BLQRouter<MockStrategy, MockStrategy>,
        message: Message<KafkaPayload>,
        max_retries: usize,
    ) -> Result<(), SubmitError<KafkaPayload>> {
        let mut msg = message;
        for _ in 0..max_retries {
            match router.submit(msg) {
                Ok(()) => return Ok(()),
                Err(SubmitError::MessageRejected(rejected)) => {
                    _ = router.poll();
                    msg = rejected.message;
                }
                Err(e) => return Err(e),
            }
        }
        Err(SubmitError::MessageRejected(
            sentry_arroyo::processing::strategies::MessageRejected { message: msg },
        ))
    }

    #[test]
    fn test_stale_to_fresh() {
        /*
        This tests that the BLQRouter properly routes stale messages to the BLQ
        and then switches back to forwarding fresh messages once the backlog is burned
         */
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.blq_enabled", json!(true)),
            ("snuba", "consumer.blq_stale_threshold_seconds", json!(10)),
            ("snuba", "consumer.blq_static_friction_seconds", json!(0)),
        ])
        .unwrap();
        let mut router = BLQRouter::new_with_strategy(MockStrategy::new(), MockStrategy::new());
        // backlog of 10 stale messages
        for _ in 0..10 {
            router
                .submit(make_message(Utc::now() - TimeDelta::minutes(1)))
                .unwrap();
            _ = router.poll();
        }
        assert_eq!(router.state, State::RoutingStale);
        assert!(!router.producer.join_called);
        // now we are back to fresh messages
        for _ in 0..5 {
            submit_with_retry(&mut router, make_message(Utc::now()), 3).unwrap();
            _ = router.poll();
        }
        assert_eq!(router.state, State::Forwarding);
        assert!(router.producer.join_called);
        assert_eq!(router.producer.submitted.len(), 10);
        assert_eq!(router.next_step.submitted.len(), 5);
    }

    #[test]
    fn test_passthrough_when_no_flag() {
        // When the feature flag is not set, stale messages should pass through
        // to next_step instead of being routed to BLQ
        init_config();
        let mut router = BLQRouter::new_with_strategy(MockStrategy::new(), MockStrategy::new());

        for _ in 0..5 {
            router
                .submit(make_message(Utc::now() - TimeDelta::minutes(1)))
                .unwrap();
            _ = router.poll();
        }

        // All stale messages went to next_step, none to producer
        assert_eq!(router.next_step.submitted.len(), 5);
        assert_eq!(router.producer.submitted.len(), 0);
        assert_eq!(router.state, State::Idle);
    }

    #[test]
    fn test_passthrough_when_flag_disabled() {
        // When the feature flag is explicitly false, stale messages should pass through
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.blq_enabled", json!(false)),
            ("snuba", "consumer.blq_stale_threshold_seconds", json!(10)),
            ("snuba", "consumer.blq_static_friction_seconds", json!(0)),
        ])
        .unwrap();
        let mut router = BLQRouter::new_with_strategy(MockStrategy::new(), MockStrategy::new());

        for _ in 0..5 {
            router
                .submit(make_message(Utc::now() - TimeDelta::minutes(1)))
                .unwrap();
            _ = router.poll();
        }

        // All stale messages went to next_step, none to producer
        assert_eq!(router.next_step.submitted.len(), 5);
        assert_eq!(router.producer.submitted.len(), 0);
        assert_eq!(router.state, State::Idle);
    }
}
