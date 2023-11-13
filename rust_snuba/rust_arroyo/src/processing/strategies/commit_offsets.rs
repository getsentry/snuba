use crate::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use crate::types::{Message, Partition};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

pub struct CommitOffsets {
    partitions: HashMap<Partition, u64>,
    last_commit_time: SystemTime,
    commit_frequency: Duration,
}
impl<T: Clone> ProcessingStrategy<T> for CommitOffsets {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        Ok(self.commit(false))
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        for (partition, offset) in message.committable() {
            self.partitions.insert(partition, offset);
        }
        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
        Ok(self.commit(true))
    }
}

impl CommitOffsets {
    pub fn new(commit_frequency: Duration) -> Self {
        CommitOffsets {
            partitions: Default::default(),
            last_commit_time: SystemTime::now(),
            commit_frequency,
        }
    }

    fn commit(&mut self, force: bool) -> Option<CommitRequest> {
        if SystemTime::now()
            > self
                .last_commit_time
                .checked_add(self.commit_frequency)
                .unwrap()
            || force
        {
            if !self.partitions.is_empty() {
                let ret = Some(CommitRequest {
                    positions: self.partitions.clone(),
                });
                self.partitions.clear();
                self.last_commit_time = SystemTime::now();
                ret
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::strategies::commit_offsets::CommitOffsets;
    use crate::processing::strategies::{CommitRequest, ProcessingStrategy};

    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use chrono::DateTime;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_commit_offsets() {
        env_logger::init();
        let partition1 = Partition::new(Topic::new("noop-commit"), 0);
        let partition2 = Partition::new(Topic::new("noop-commit"), 1);
        let timestamp = DateTime::from(SystemTime::now());

        let m1 = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                partition: partition1.clone(),
                offset: 1000,
                payload: KafkaPayload {
                    key: None,
                    headers: None,
                    payload: None,
                },
                timestamp,
            }),
        };

        let m2 = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                partition: partition2.clone(),
                offset: 2000,
                payload: KafkaPayload {
                    key: None,
                    headers: None,
                    payload: None,
                },
                timestamp,
            }),
        };

        let mut noop = CommitOffsets::new(Duration::from_secs(1));

        let mut commit_req1 = CommitRequest {
            positions: Default::default(),
        };
        commit_req1.positions.insert(partition1, 1001);
        noop.submit(m1).expect("Failed to submit");
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::poll(&mut noop).unwrap(),
            None
        );

        sleep(Duration::from_secs(2));
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::poll(&mut noop).unwrap(),
            Some(commit_req1)
        );

        let mut commit_req2 = CommitRequest {
            positions: Default::default(),
        };
        commit_req2.positions.insert(partition2, 2001);
        noop.submit(m2).expect("Failed to submit");
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::poll(&mut noop).unwrap(),
            None
        );
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::join(
                &mut noop,
                Some(Duration::from_secs(5))
            )
            .unwrap(),
            Some(commit_req2)
        );
    }
}
