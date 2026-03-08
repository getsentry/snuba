use std::collections::HashMap;
use std::time::Duration;

use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};

use crate::types::AggregatedOutcomesBatch;

pub struct CommitOutcomes<TNext> {
    next_step: TNext,
    /// Accumulated max committable offset per partition, drained on each poll/join.
    commit_positions: HashMap<Partition, u64>,
}

impl<TNext> CommitOutcomes<TNext> {
    pub fn new(next_step: TNext) -> Self {
        Self {
            next_step,
            commit_positions: HashMap::new(),
        }
    }

    fn take_commit_request(&mut self) -> Option<CommitRequest> {
        if self.commit_positions.is_empty() {
            return None;
        }
        Some(CommitRequest {
            positions: self.commit_positions.drain().collect(),
        })
    }
}

impl<TNext: ProcessingStrategy<AggregatedOutcomesBatch>> ProcessingStrategy<AggregatedOutcomesBatch>
    for CommitOutcomes<TNext>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()?;
        Ok(self.take_commit_request())
    }

    fn submit(
        &mut self,
        message: Message<AggregatedOutcomesBatch>,
    ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
        for (partition, offset) in message.committable() {
            self.commit_positions
                .entry(partition)
                .and_modify(|o| *o = (*o).max(offset))
                .or_insert(offset);
        }
        self.next_step.submit(message)
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)?;
        Ok(self.take_commit_request())
    }
}
