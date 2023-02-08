use crate::arroyo::backends::kafka::types::KafkaPayload;
use crate::arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use crate::arroyo::types::{Message, Partition, Position};
use log::info;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

pub struct NoopCommit {
    partitions: HashMap<Partition, Position>,
    last_commit_time: SystemTime,
    commit_frequency: Duration,
}
impl ProcessingStrategy<KafkaPayload> for NoopCommit {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.commit(false)
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        let next_offset = message.next_offset();
        self.partitions.insert(
            message.partition,
            Position {
                offset: next_offset,
                timestamp: message.timestamp,
            },
        );
        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Option<CommitRequest> {
        self.commit(true)
    }
}

impl NoopCommit {
    fn commit(&mut self, force: bool) -> Option<CommitRequest> {
        if SystemTime::now()
            > self
                .last_commit_time
                .checked_add(self.commit_frequency)
                .unwrap()
            || force
        {
            info!("Performing a commit");
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

pub fn new(commit_frequency: Duration) -> NoopCommit {
    NoopCommit {
        partitions: Default::default(),
        last_commit_time: SystemTime::now(),
        commit_frequency,
    }
}

#[cfg(test)]
mod tests {
    use crate::arroyo::backends::kafka::types::KafkaPayload;
    use crate::arroyo::processing::strategies::{noop, CommitRequest, ProcessingStrategy};
    use crate::arroyo::types::{Message, Partition, Position, Topic};
    use chrono::DateTime;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_noop() {
        env_logger::init();
        let partition1 = Partition {
            topic: Topic {
                name: "noop-commit".to_string(),
            },
            index: 0,
        };
        let partition2 = Partition {
            topic: Topic {
                name: "noop-commit".to_string(),
            },
            index: 1,
        };
        let timestamp = DateTime::from(SystemTime::now());
        let m1 = Message {
            partition: partition1.clone(),
            offset: 1000,
            payload: KafkaPayload {
                key: None,
                headers: None,
                payload: None,
            },
            timestamp,
        };
        let m2 = Message {
            partition: partition2.clone(),
            offset: 2000,
            payload: KafkaPayload {
                key: None,
                headers: None,
                payload: None,
            },
            timestamp,
        };

        let mut noop = noop::new(Duration::from_secs(1));

        let mut commit_req1 = CommitRequest {
            positions: Default::default(),
        };
        commit_req1.positions.insert(
            partition1,
            Position {
                offset: 1001,
                timestamp,
            },
        );
        noop.submit(m1).expect("Failed to submit");
        assert_eq!(noop.poll(), None);

        sleep(Duration::from_secs(2));
        assert_eq!(noop.poll(), Some(commit_req1));

        let mut commit_req2 = CommitRequest {
            positions: Default::default(),
        };
        commit_req2.positions.insert(
            partition2,
            Position {
                offset: 2001,
                timestamp,
            },
        );
        noop.submit(m2).expect("Failed to submit");
        assert_eq!(noop.poll(), None);
        assert_eq!(noop.join(Some(Duration::from_secs(5))), Some(commit_req2))
    }
}
