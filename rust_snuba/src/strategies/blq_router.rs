use std::time::Duration;

use chrono::{TimeDelta, Utc};
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};
// todo: config params
// concurrency config
// commit offset frequency

// Once we enter stale routing mode (at STALE_THRESHOLD),
// we keep routing messages that are at least (STALE_THRESHOLD - STATIC_FRICTION_SECS) seconds old.
// This is because we want a higher threshold to enter the stale routing state
// but a lower threshold to stay in it, so we don't flip-flop at the boundary.
const STATIC_FRICTION_SECS: i64 = 180;

#[derive(PartialEq)]

enum State {
    Idle,         // no messages have gone through the router yet
    RoutingStale, // router is directing stale messages to the backlog-queue (BLQ)
    Forwarding,   // router is forwarding non-stale messages along to the next strategy
}

pub struct BLQConfig {
    // if message timestamp is older than stale_threshold its routed to the backlog-queue
    pub stale_threshold: TimeDelta,
    pub blq_producer: KafkaProducer,
    pub blq_topic: Topic,
}

pub struct BLQRouter<Next> {
    next_step: Next,
    stale_threshold: TimeDelta,
    state: State,
    producer: Produce<CommitOffsets>,
    _concurrency: ConcurrencyConfig, // you should never have to deal w this, its just needed for lifetimes
}

impl<Next> BLQRouter<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    pub fn new(next_step: Next, config: BLQConfig) -> Self {
        // next_step: where fresh messages get forwarded to
        let concurrency = ConcurrencyConfig::new(10);
        let producer = Produce::new(
            CommitOffsets::new(Duration::from_millis(250)),
            config.blq_producer,
            &concurrency,
            TopicOrPartition::Topic(config.blq_topic),
        );
        Self {
            next_step,
            stale_threshold: config.stale_threshold,
            state: State::Idle,
            producer,
            _concurrency: concurrency,
        }
    }
}

impl<Next> ProcessingStrategy<KafkaPayload> for BLQRouter<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let producer_result = self.producer.poll();
        let next_step_result = self.next_step.poll();
        match self.state {
            State::RoutingStale => producer_result,
            State::Forwarding | State::Idle => next_step_result,
        }
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        let msg_ts = message
            .timestamp()
            .expect("Expected kafka message to always have a timestamp, but there wasn't one");
        let elapsed = Utc::now() - msg_ts;
        let is_stale = match &self.state {
            State::RoutingStale => {
                // see STATIC_FRICTION_SECS
                elapsed > (self.stale_threshold - TimeDelta::seconds(STATIC_FRICTION_SECS))
            }
            _ => elapsed > self.stale_threshold,
        };
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
                // Call join on the producer so all writes to the BLQ are committed.
                self.producer.join(Some(Duration::from_secs(5))).unwrap();

                // Now go back to forwarding non-stale messages as usual.
                self.state = State::Forwarding;
                self.next_step.submit(message)
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
        match self.state {
            State::RoutingStale => producer_result,
            State::Forwarding | State::Idle => next_step_result,
        }
    }
}
