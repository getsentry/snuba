use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{AnyMessage, InnerMessage, Message, Partition};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

struct BatchState<T> {
    offsets: BTreeMap<Partition, u64>,
    values: Vec<T>,
    batch_start_time: SystemTime,
    is_complete: bool,
}

impl<T: Clone> BatchState<T> {
    fn new() -> BatchState<T> {
        BatchState {
            offsets: Default::default(),
            values: vec![],
            batch_start_time: SystemTime::now(),
            is_complete: false,
        }
    }

    fn append(&mut self, message: Message<T>) {
        self.values.push(message.payload());
        for (partition, offset) in message.committable() {
            self.offsets.insert(partition, offset);
        }
    }
}

pub struct Batch<T> {
    next_step: Box<dyn ProcessingStrategy<Vec<T>>>,
    max_batch_size: usize,
    max_batch_time: Duration,
    batch_state: BatchState<T>,
}
impl<T: Clone + Send + Sync> ProcessingStrategy<T> for Batch<T> {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.flush(false)
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), MessageRejected> {
        if self.batch_state.is_complete {
            return Err(MessageRejected);
        }
        self.batch_state.append(message);

        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Option<CommitRequest> {
        self.flush(true)
    }
}

impl <T: Clone>Batch<T> {
    #[allow(dead_code)]
    pub fn new(
        next_step: Box<dyn ProcessingStrategy<Vec<T>>>,
        max_batch_size: usize,
        max_batch_time: Duration,
    ) -> Batch<T> {
        Batch {
            next_step,
            max_batch_size,
            max_batch_time,
            batch_state: BatchState::new(),
        }
    }

    fn flush(&mut self, force: bool) -> Option<CommitRequest> {
        let batch_complete = self.batch_state.values.len() >= self.max_batch_size
            || self.batch_state.batch_start_time.elapsed().unwrap() > self.max_batch_time;

        if batch_complete || force {
            let next_message = Message {
                inner_message: InnerMessage::AnyMessage(AnyMessage::new(
                    self.batch_state.values.clone(),
                    self.batch_state.offsets.clone(),
                )),
            };

            match self.next_step.submit(next_message) {
                Ok(_) => {
                    self.batch_state = BatchState::new();
                }
                Err(MessageRejected) => {
                    // The batch is marked is_complete, and we stop accepting
                    // messages until the batch can be sucessfully submitted to the next step.
                    self.batch_state.is_complete = true;
                }
            }
        }

        None
    }
}


#[cfg(test)]
mod tests {
    use rust_arroyo::processing::strategies::{noop, ProcessingStrategy};
    use crate::strategies::batch::Batch;
    use chrono;


    use rust_arroyo::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use std::time::{Duration};

    #[test]
    fn test_batch() {
        env_logger::init();
        let partition1 = Partition {
            topic: Topic {
                name: "noop-commit".to_string(),
            },
            index: 0,
        };

        let max_batch_size = 10;
        let max_batch_time = Duration::from_secs(1);

        let mut strategy: Batch<u64> = Batch::new(Box::new(noop::new(max_batch_time)), max_batch_size, max_batch_time);
        let msg = Message {inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(1, partition1, 1, chrono::Utc::now()))};
        strategy.submit(msg).unwrap();
        strategy.poll();
        strategy.close();
        strategy.join(None);
    }
}
