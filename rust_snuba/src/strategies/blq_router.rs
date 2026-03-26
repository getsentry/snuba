use std::time::Duration;

use chrono::{TimeDelta, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

#[derive(PartialEq)]

enum State {
    Empty,
    BatchingStale,
    BatchingFresh,
}

pub struct BLQRouter<Next> {
    next_step: Next,
    stale_threshold: TimeDelta,
    state: State,
}

impl<Next> BLQRouter<Next> {
    pub fn new(next_step: Next, stale_threshold: TimeDelta) -> Self {
        Self {
            next_step,
            stale_threshold,
            state: State::Empty,
        }
    }
}

impl<Next> ProcessingStrategy<KafkaPayload> for BLQRouter<Next>
where
    Next: ProcessingStrategy<KafkaPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
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

                // todo: batch stale
                println!("batching stale");

                // all stale message handling happens in this strategy,
                // nothing should be passed downstream
                Ok(())
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

                // todo: commit stale
                println!("commit stale batch");

                println!("batch fresh");
                self.state = State::BatchingFresh;
                self.next_step.submit(message)
            }
        }
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
    }
}
