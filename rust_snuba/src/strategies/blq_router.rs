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

#[derive(PartialEq)]

enum State {
    Empty,
    BatchingStale,
    BatchingFresh,
}

// todo: config params
// concurrency config
// commit offset frequency

pub struct BLQConfig {
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
            state: State::Empty,
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
            State::BatchingStale => producer_result,
            State::BatchingFresh | State::Empty => next_step_result,
        }
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        let msg_ts = message
            .timestamp()
            .expect("Expected kafka message to always have a timestamp, but there wasn't one");
        let elapsed = Utc::now() - msg_ts;
        let is_stale = elapsed > self.stale_threshold;
        match (is_stale, &self.state) {
            (true, State::BatchingFresh) => {
                // we want the consumer to crash
                // this is the only way for us to drop the batch of fresh messages without commiting
                // when the consumer restarts it will get the stale message again in State::Empty
                panic!("Resetting consumer state to begin processing the stale backlog")
            }
            (true, State::Empty) | (true, State::BatchingStale) => {
                // batch stale messages
                if self.state == State::Empty {
                    self.state = State::BatchingStale;
                }
                println!("batch stale");
                self.producer.submit(message)
            }
            (false, State::Empty) | (false, State::BatchingFresh) => {
                // batch fresh messages
                println!("batch fresh");
                if self.state == State::Empty {
                    self.state = State::BatchingFresh;
                }
                self.next_step.submit(message)
            }
            (false, State::BatchingStale) => {
                // we hit a fresh message, so we commit our stale batch
                // and start batching the fresh messages

                // the producer should finish writing and committing everything
                // if  it doesnt finish all commits we cant move on to the next offsets
                println!("joining the stale batch");
                self.producer.join(Some(Duration::from_secs(5))).unwrap();

                println!("batch fresh");
                self.state = State::BatchingFresh;
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
            State::BatchingStale => producer_result,
            State::BatchingFresh | State::Empty => next_step_result,
        }
    }
}
