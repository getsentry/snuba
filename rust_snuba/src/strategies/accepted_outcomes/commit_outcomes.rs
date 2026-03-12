use std::collections::HashMap;
use std::time::{Duration, Instant};

use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};

use crate::types::AggregatedOutcomesBatch;

pub struct CommitOutcomes<TNext> {
    next_step: TNext,
    /// Accumulated max committable offset per partition, drained on each poll/join.
    commit_positions: HashMap<Partition, u64>,
    // adding a bit more time before we commit since producing will take some time
    // and we want to allow some time to handle an error from produce's `poll()`
    last_commit_time: Instant,
    commit_frequency: Duration,
}

impl<TNext> CommitOutcomes<TNext> {
    pub fn new(next_step: TNext, commit_frequency: Option<Duration>) -> Self {
        Self {
            next_step,
            commit_positions: Default::default(),
            last_commit_time: Instant::now(),
            // use the passed in commit frequency or default to once every 10 seconds
            commit_frequency: commit_frequency.unwrap_or(Duration::from_secs(10)),
        }
    }

    fn take_commit_request(&mut self, force: bool) -> Option<CommitRequest> {
        if self.commit_positions.is_empty() {
            return None;
        }

        if Instant::now() - self.last_commit_time <= self.commit_frequency && !force {
            return None;
        }

        let req = Some(CommitRequest {
            positions: self.commit_positions.clone(),
        });

        self.commit_positions.clear();
        self.last_commit_time = Instant::now();
        req
    }
}

impl<TNext: ProcessingStrategy<AggregatedOutcomesBatch>> ProcessingStrategy<AggregatedOutcomesBatch>
    for CommitOutcomes<TNext>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        // Produce step never returns a commit request so we don't expect
        // it here, we just want to make sure we bubble up errors
        self.next_step.poll()?;
        Ok(self.take_commit_request(false))
    }

    fn submit(
        &mut self,
        message: Message<AggregatedOutcomesBatch>,
    ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
        // We only to self.commit_positions if we don't get any errors from
        // submitting to the produce step
        let committable: Vec<(Partition, u64)> = message.committable().collect();
        match self.next_step.submit(message) {
            Ok(()) => {
                for (partition, offset) in committable {
                    self.commit_positions
                        .entry(partition)
                        .and_modify(|o| *o = (*o).max(offset))
                        .or_insert(offset);
                }
                Ok(())
            }
            // We want to bubble up MessageRejected and InvalidMessages from the produce step
            // MessageRejected will only happen when we reach max threads (--concurrency param)
            // in the produce RunTaskInThreads step
            Err(e) => Err(e),
        }
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        // wait until the produce step is done
        self.next_step.join(timeout)?;
        Ok(self.take_commit_request(true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentry_arroyo::processing::strategies::MessageRejected;
    use sentry_arroyo::types::Topic;

    struct AlwaysReject;
    impl ProcessingStrategy<AggregatedOutcomesBatch> for AlwaysReject {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
        fn submit(
            &mut self,
            message: Message<AggregatedOutcomesBatch>,
        ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
            Err(SubmitError::MessageRejected(MessageRejected { message }))
        }
        fn terminate(&mut self) {}
        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    struct AlwaysAccept;
    impl ProcessingStrategy<AggregatedOutcomesBatch> for AlwaysAccept {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
        fn submit(
            &mut self,
            _message: Message<AggregatedOutcomesBatch>,
        ) -> Result<(), SubmitError<AggregatedOutcomesBatch>> {
            Ok(())
        }
        fn terminate(&mut self) {}
        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    fn make_message(partition: Partition, offset: u64) -> Message<AggregatedOutcomesBatch> {
        use std::collections::BTreeMap;
        let mut committable = BTreeMap::new();
        committable.insert(partition, offset);
        Message::new_any_message(AggregatedOutcomesBatch::default(), committable)
    }

    #[test]
    fn message_rejected_does_not_track_offsets() {
        let mut strategy = CommitOutcomes::new(AlwaysReject, None);
        let partition = Partition::new(Topic::new("test"), 0);
        let msg = make_message(partition, 5);

        let result = strategy.submit(msg);
        assert!(matches!(result, Err(SubmitError::MessageRejected(_))));
        assert!(strategy.commit_positions.is_empty());
    }

    #[test]
    fn successful_submit_tracks_offsets() {
        let mut strategy = CommitOutcomes::new(AlwaysAccept, None);
        let topic = Topic::new("test");
        let partition = Partition::new(topic, 0);

        strategy.submit(make_message(partition, 3)).unwrap();
        assert_eq!(strategy.commit_positions.get(&partition).copied(), Some(3));

        // Submitting a higher offset should update the tracked position
        strategy.submit(make_message(partition, 10)).unwrap();
        assert_eq!(strategy.commit_positions.get(&partition).copied(), Some(10));
    }
}
