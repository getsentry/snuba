use crate::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    SubmitError,
};
use crate::types::{Message, Partition};
use crate::utils::metrics::{get_metrics, BoxMetrics};
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

struct BatchState<T, TResult> {
    value: Option<TResult>,
    accumulator: Arc<dyn Fn(TResult, T) -> TResult + Send + Sync>,
    offsets: BTreeMap<Partition, u64>,
    batch_start_time: SystemTime,
    message_count: usize,
}

impl<T, TResult> BatchState<T, TResult> {
    fn new(
        initial_value: TResult,
        accumulator: Arc<dyn Fn(TResult, T) -> TResult + Send + Sync>,
    ) -> BatchState<T, TResult> {
        BatchState {
            value: Some(initial_value),
            accumulator,
            offsets: Default::default(),
            batch_start_time: SystemTime::now(),
            message_count: 0,
        }
    }

    fn add(&mut self, message: Message<T>) {
        for (partition, offset) in message.committable() {
            self.offsets.insert(partition, offset);
        }

        let tmp = self.value.take().unwrap();
        self.value = Some((self.accumulator)(tmp, message.into_payload()));
        self.message_count += 1;
    }
}

pub struct Reduce<T, TResult> {
    next_step: Box<dyn ProcessingStrategy<TResult>>,
    accumulator: Arc<dyn Fn(TResult, T) -> TResult + Send + Sync>,
    initial_value: TResult,
    max_batch_size: usize,
    max_batch_time: Duration,
    batch_state: BatchState<T, TResult>,
    message_carried_over: Option<Message<TResult>>,
    commit_request_carried_over: Option<CommitRequest>,
    metrics: BoxMetrics,
}

impl<T: Send + Sync, TResult: Clone + Send + Sync> ProcessingStrategy<T> for Reduce<T, TResult> {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        self.flush(false)?;

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        self.batch_state.add(message);

        Ok(())
    }

    fn close(&mut self) {
        self.next_step.close();
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
        let start = Instant::now();
        if self.message_carried_over.is_some() {
            while self.message_carried_over.is_some() {
                let next_commit = self.next_step.poll()?;
                self.commit_request_carried_over =
                    merge_commit_request(self.commit_request_carried_over.take(), next_commit);
                self.flush(true)?;
                if let Some(t) = timeout {
                    if start.elapsed() > t {
                        tracing::warn!("Timeout reached while waiting for tasks to finish");
                        break;
                    }
                }
            }
        } else {
            self.flush(true)?;
        }

        let remaining: Option<Duration> =
            timeout.map(|t| t.checked_sub(start.elapsed()).unwrap_or(Duration::ZERO));

        let next_commit = self.next_step.join(remaining)?;

        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

impl<T, TResult: Clone> Reduce<T, TResult> {
    pub fn new(
        next_step: Box<dyn ProcessingStrategy<TResult>>,
        accumulator: Arc<dyn Fn(TResult, T) -> TResult + Send + Sync>,
        initial_value: TResult,
        max_batch_size: usize,
        max_batch_time: Duration,
    ) -> Reduce<T, TResult> {
        let batch_state = BatchState::new(initial_value.clone(), accumulator.clone());
        Reduce {
            next_step,
            accumulator,
            initial_value,
            max_batch_size,
            max_batch_time,
            batch_state,
            message_carried_over: None,
            commit_request_carried_over: None,
            metrics: get_metrics(),
        }
    }

    fn flush(&mut self, force: bool) -> Result<(), InvalidMessage> {
        // Try re-submitting the carried over message if there is one
        if let Some(message) = self.message_carried_over.take() {
            match self.next_step.submit(message) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message);
                }
                Ok(_) => {}
            }
        }

        if self.batch_state.message_count == 0 {
            return Ok(());
        }

        let batch_time = self.batch_state.batch_start_time.elapsed().ok();
        let batch_complete = self.batch_state.message_count >= self.max_batch_size
            || batch_time.unwrap_or_default() > self.max_batch_time;

        if !batch_complete && !force {
            return Ok(());
        }

        if let Some(batch_time) = batch_time {
            self.metrics.timing(
                "arroyo.strategies.reduce.batch_time",
                batch_time.as_secs(),
                None,
            );
        }

        let batch_state = mem::replace(
            &mut self.batch_state,
            BatchState::new(self.initial_value.clone(), self.accumulator.clone()),
        );

        let next_message =
            Message::new_any_message(batch_state.value.unwrap(), batch_state.offsets);

        match self.next_step.submit(next_message) {
            Err(SubmitError::MessageRejected(MessageRejected {
                message: transformed_message,
            })) => {
                self.message_carried_over = Some(transformed_message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(invalid_message)) => Err(invalid_message),
            Ok(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::strategies::reduce::Reduce;
    use crate::processing::strategies::{
        CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
    };
    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_reduce() {
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();

        struct NextStep<T> {
            pub submitted: Arc<Mutex<Vec<T>>>,
        }

        impl<T: Send + Sync> ProcessingStrategy<T> for NextStep<T> {
            fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }

            fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
                self.submitted.lock().unwrap().push(message.into_payload());
                Ok(())
            }

            fn close(&mut self) {}

            fn terminate(&mut self) {}

            fn join(
                &mut self,
                _: Option<Duration>,
            ) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
        }

        let partition1 = Partition::new(Topic::new("test"), 0);

        let max_batch_size = 2;
        let max_batch_time = Duration::from_secs(1);

        let initial_value = Vec::new();
        let accumulator = Arc::new(|mut acc: Vec<u64>, value: u64| {
            acc.push(value);
            acc
        });

        let next_step = Box::new(NextStep {
            submitted: submitted_messages,
        });

        let mut strategy: Reduce<u64, Vec<u64>> = Reduce::new(
            next_step,
            accumulator,
            initial_value,
            max_batch_size,
            max_batch_time,
        );

        for i in 0..3 {
            let msg = Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    i,
                    partition1,
                    i,
                    chrono::Utc::now(),
                )),
            };
            strategy.submit(msg).unwrap();
            let _ = strategy.poll();
        }

        strategy.close();
        let _ = strategy.join(None);

        // 2 batches were created
        assert_eq!(
            *submitted_messages_clone.lock().unwrap(),
            vec![vec![0, 1], vec![2]]
        );
    }
}
