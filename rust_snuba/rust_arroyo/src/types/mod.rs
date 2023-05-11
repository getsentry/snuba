use chrono::{DateTime, Utc};
use std::any::type_name;
use std::cmp::Eq;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::Hash;

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Topic {
    pub name: String,
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic({})", self.name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Partition {
    // TODO: Make this a reference to 'static Topic.
    pub topic: Topic,
    pub index: u16,
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Partition({} topic={})", self.index, &self.topic)
    }
}

#[derive(PartialEq)]
pub enum TopicOrPartition {
    Topic(Topic),
    Partition(Partition),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerMessage<T: Clone> {
    pub payload: T,
    pub partition: Partition,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

impl<T: Clone> BrokerMessage<T> {
    pub fn new(payload: T, partition: Partition, offset: u64, timestamp: DateTime<Utc>) -> Self {
        Self {
            payload,
            partition,
            offset,
            timestamp,
        }
    }

    pub fn replace<TReplaced: Clone>(self, replacement: TReplaced) -> BrokerMessage<TReplaced> {
        BrokerMessage {
            payload: replacement,
            partition: self.partition,
            offset: self.offset,
            timestamp: self.timestamp,
        }
    }
}

impl<T: Clone> fmt::Display for BrokerMessage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrokerMessage(partition={} offset={})",
            self.partition, self.offset
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnyMessage<T: Clone> {
    pub payload: T,
    pub committable: BTreeMap<Partition, u64>,
}

impl<T: Clone> AnyMessage<T> {
    pub fn new(payload: T, committable: BTreeMap<Partition, u64>) -> Self {
        Self {
            payload,
            committable,
        }
    }

    pub fn replace<TReplaced: Clone>(self, replacement: TReplaced) -> AnyMessage<TReplaced> {
        AnyMessage {
            payload: replacement,
            committable: self.committable,
        }
    }
}

impl<T: Clone> fmt::Display for AnyMessage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AnyMessage(committable={:?})", self.committable)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum InnerMessage<T: Clone> {
    BrokerMessage(BrokerMessage<T>),
    AnyMessage(AnyMessage<T>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Message<T: Clone> {
    pub inner_message: InnerMessage<T>,
}

impl<T: Clone> Message<T> {
    pub fn payload(&self) -> T {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage { payload, .. }) => payload.clone(),
            InnerMessage::AnyMessage(AnyMessage { payload, .. }) => payload.clone(),
        }
    }

    pub fn committable(&self) -> BTreeMap<Partition, u64> {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage {
                partition, offset, ..
            }) => {
                let mut map = BTreeMap::new();
                // TODO: Get rid of the clone
                map.insert(partition.clone(), offset + 1);
                map
            }
            InnerMessage::AnyMessage(AnyMessage { committable, .. }) => committable.clone(),
        }
    }

    pub fn replace<TReplaced: Clone>(self, replacement: TReplaced) -> Message<TReplaced> {
        match self.inner_message {
            InnerMessage::BrokerMessage(inner) => Message {
                inner_message: InnerMessage::BrokerMessage(inner.replace(replacement)),
            },
            InnerMessage::AnyMessage(inner) => Message {
                inner_message: InnerMessage::AnyMessage(inner.replace(replacement)),
            },
        }
    }
}

impl<T: Clone> fmt::Display for Message<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage {
                partition, offset, ..
            }) => {
                write!(
                    f,
                    "Message<{}>(partition={}), offset={}",
                    type_name::<T>(),
                    &partition,
                    &offset
                )
            }
            InnerMessage::AnyMessage(AnyMessage { committable, .. }) => {
                write!(
                    f,
                    "Message<{}>(committable={})",
                    type_name::<T>(),
                    &committable
                        .iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BrokerMessage, Partition, Topic};
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn message() {
        let now = Utc::now();
        let topic = Topic {
            name: "test".to_string(),
        };
        let part = Partition { topic, index: 10 };
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);

        assert_eq!(message.partition.topic.name, "test");
        assert_eq!(message.partition.index, 10);
        assert_eq!(message.offset, 10);
        assert_eq!(message.payload, "payload");
        assert_eq!(message.timestamp, now);
    }

    #[test]
    fn fmt_display() {
        let now = Utc::now();
        let part = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 10,
        };
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);

        assert_eq!(
            message.to_string(),
            "BrokerMessage(partition=Partition(10 topic=Topic(test)) offset=10)"
        )
    }

    #[test]
    fn test_eq() {
        let a = Topic {
            name: "test".to_string(),
        };
        let b = Topic {
            name: "test".to_string(),
        };
        assert!(a == b);

        let c = Topic {
            name: "test2".to_string(),
        };
        assert!(a != c);
    }

    #[test]
    fn test_hash() {
        let mut content = HashMap::new();
        content.insert(
            Topic {
                name: "test".to_string(),
            },
            "test_value".to_string(),
        );

        let b = Topic {
            name: "test".to_string(),
        };
        let c = content.get(&b).unwrap();
        assert_eq!(&"test_value".to_string(), c);
    }

    #[test]
    fn test_clone() {
        let topic = Topic {
            name: "test".to_string(),
        };
        let part = Partition { topic, index: 10 };

        let part2 = part.clone();
        assert_eq!(part, part2);
        assert_ne!(&part as *const Partition, &part2 as *const Partition);

        let now = Utc::now();
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);
        let message2 = message.clone();

        assert_eq!(message, message2);
        assert_ne!(
            &message as *const BrokerMessage<String>,
            &message2 as *const BrokerMessage<String>
        );
    }
}
