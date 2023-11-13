#![allow(dead_code)]
use crate::types::{BrokerMessage, Partition};
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};

/// Stores messages that are pending commit. This is used to retreive raw messages
// in case they need to be placed in the DLQ.
pub struct BufferedMessages<TPayload> {
    buffered_messages: BTreeMap<Partition, VecDeque<BrokerMessage<TPayload>>>,
}

impl<TPayload> BufferedMessages<TPayload> {
    pub fn new() -> Self {
        BufferedMessages {
            buffered_messages: BTreeMap::new(),
        }
    }

    // Add message to the buffer.
    pub fn append(&mut self, message: BrokerMessage<TPayload>) {
        match self.buffered_messages.get_mut(&message.partition) {
            Some(messages) => {
                messages.push_back(message);
            }
            None => {
                self.buffered_messages
                    .insert(message.partition.clone(), VecDeque::from([message]));
            }
        };
    }

    // Return the message at the given offset or None if it is not found in the buffer.
    // Messages up to the offset for the given partition are removed.
    pub fn pop(&mut self, partition: &Partition, offset: u64) -> Option<BrokerMessage<TPayload>> {
        if let Some(messages) = self.buffered_messages.get_mut(partition) {
            #[allow(clippy::never_loop)]
            while let Some(message) = messages.front_mut() {
                match message.offset.cmp(&offset) {
                    Ordering::Equal => {
                        return messages.pop_front();
                    }
                    Ordering::Greater => {
                        break;
                    }
                    Ordering::Less => {
                        messages.pop_front();
                    }
                };
            }
        }

        None
    }

    // Clear the buffer. Should be called on rebalance.
    pub fn reset(&mut self) {
        self.buffered_messages.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Topic;
    use chrono::Utc;

    use super::*;

    #[test]
    fn test_buffered_messages() {
        let mut buffer = BufferedMessages::new();
        let partition = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition: partition.clone(),
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        assert_eq!(buffer.pop(&partition, 0).unwrap().offset, 0);
        assert_eq!(buffer.pop(&partition, 8).unwrap().offset, 8);
        assert_eq!(buffer.pop(&partition, 1), None); // Removed when we popped offset 8
        assert_eq!(buffer.pop(&partition, 9).unwrap().offset, 9);
        assert_eq!(buffer.pop(&partition, 10), None); // Doesn't exist
    }
}
