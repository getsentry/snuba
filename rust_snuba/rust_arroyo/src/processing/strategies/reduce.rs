use crate::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy,
};
use crate::types::{AnyMessage, InnerMessage, Message, Partition};
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

impl<T: Clone, TResult: Clone> BatchState<T, TResult> {
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
        let tmp = self.value.take();
        self.value = Some((self.accumulator)(tmp.unwrap(), message.payload()));
        self.message_count += 1;

        for (partition, offset) in message.committable() {
            self.offsets.insert(partition, offset);
        }
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
}
impl<T: Clone + Send + Sync, TResult: Clone + Send + Sync> ProcessingStrategy<T>
    for Reduce<T, TResult>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        self.flush(false);
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), MessageRejected<T>> {
        if self.message_carried_over.is_some() {
            return Err(MessageRejected { message });
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

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        let start = Instant::now();
        let mut remaining: Option<Duration> = timeout;
        let mut commit_request = None;
        if self.message_carried_over.is_some() {
            while self.message_carried_over.is_some() {
                commit_request = merge_commit_request(commit_request, self.next_step.poll());
                self.flush(true);
                if let Some(t) = remaining {
                    if t <= Duration::from_secs(0) {
                        log::warn!("Timeout reached while waiting for tasks to finish");
                        break;
                    }
                    remaining = Some(t - start.elapsed());
                }
            }
        } else {
            self.flush(true);
        }
        let next_commit = self.next_step.join(remaining);
        merge_commit_request(commit_request, next_commit)
    }
}

impl<T: Clone + Send + Sync, TResult: Clone + Send + Sync> Reduce<T, TResult> {
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
        }
    }

    fn flush(&mut self, force: bool) {
        // Try re-submitting the carried over message if there is one
        if let Some(message) = self.message_carried_over.take() {
            if let Err(MessageRejected {
                message: transformed_message,
            }) = self.next_step.submit(message)
            {
                self.message_carried_over = Some(transformed_message);
                return;
            }
        }

        if self.batch_state.message_count == 0 {
            return;
        }

        let batch_complete = self.batch_state.message_count >= self.max_batch_size
            || self
                .batch_state
                .batch_start_time
                .elapsed()
                .unwrap_or_default()
                > self.max_batch_time;

        if batch_complete || force {
            let batch_state = mem::replace(
                &mut self.batch_state,
                BatchState::new(self.initial_value.clone(), self.accumulator.clone()),
            );

            let next_message = Message {
                inner_message: InnerMessage::AnyMessage(AnyMessage::new(
                    batch_state.value.unwrap(),
                    batch_state.offsets,
                )),
            };

            if let Err(MessageRejected {
                message: transformed_message,
            }) = self.next_step.submit(next_message)
            {
                self.message_carried_over = Some(transformed_message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::strategies::reduce::Reduce;
    use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
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

        impl<T: Clone + Send + Sync> ProcessingStrategy<T> for NextStep<T> {
            fn poll(&mut self) -> Option<CommitRequest> {
                None
            }

            fn submit(&mut self, message: Message<T>) -> Result<(), MessageRejected<T>> {
                self.submitted.lock().unwrap().push(message.payload());
                Ok(())
            }

            fn close(&mut self) {}

            fn terminate(&mut self) {}

            fn join(&mut self, _: Option<Duration>) -> Option<CommitRequest> {
                None
            }
        }

        let partition1 = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };

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
                    partition1.clone(),
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
