use std::time::Duration;

use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

pub struct SetJoinTimeout<N> {
    next_step: N,
    new_timeout: Option<Duration>,
}

impl<N> SetJoinTimeout<N> {
    pub fn new(next_step: N, new_timeout: Option<Duration>) -> Self {
        SetJoinTimeout {
            new_timeout,
            next_step,
        }
    }
}

impl<N, TPayload> ProcessingStrategy<TPayload> for SetJoinTimeout<N>
where
    N: ProcessingStrategy<TPayload>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        self.next_step.submit(message)
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, _timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(self.new_timeout)
    }
}
