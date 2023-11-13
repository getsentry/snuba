use std::any::type_name;
use std::cmp::Eq;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::Hash;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use indexmap::IndexSet;
use once_cell::sync::Lazy;

static INTERNED_TOPICS: Lazy<RwLock<IndexSet<String>>> = Lazy::new(Default::default);

#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Topic(u16);

impl Topic {
    pub fn new(name: &str) -> Self {
        let mut interner = INTERNED_TOPICS.write().unwrap();
        let (idx, _) = interner.insert_full(name.into());
        let idx = idx
            .try_into()
            .expect("exceeded maximum number of unique topics");
        Self(idx)
    }

    pub fn as_str(&self) -> &str {
        let interner = INTERNED_TOPICS.read().unwrap();
        let s = interner
            .get_index(self.0 as usize)
            .expect("invalid internet `Topic`");

        // SAFETY:
        // - The interner is static and append-only, so it essentially leaks.
        // - We insert heap-allocated `String`s that do not move.
        unsafe { std::mem::transmute::<&str, &'static str>(s) }
    }
}

impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.as_str();
        f.debug_tuple("Topic").field(&s).finish()
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic({})", self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Partition {
    pub topic: Topic,
    pub index: u16,
}

impl Partition {
    pub fn new(topic: Topic, index: u16) -> Self {
        Self { topic, index }
    }
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
pub struct BrokerMessage<T> {
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
pub struct AnyMessage<T> {
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
pub enum InnerMessage<T> {
    BrokerMessage(BrokerMessage<T>),
    AnyMessage(AnyMessage<T>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Message<T> {
    pub inner_message: InnerMessage<T>,
}

impl<T: Clone> Message<T> {
    pub fn new_broker_message(
        payload: T,
        partition: Partition,
        offset: u64,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                payload,
                partition,
                offset,
                timestamp,
            }),
        }
    }

    pub fn new_any_message(payload: T, committable: BTreeMap<Partition, u64>) -> Self {
        Self {
            inner_message: InnerMessage::AnyMessage(AnyMessage {
                payload,
                committable,
            }),
        }
    }

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
                map.insert(*partition, offset + 1);
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

    #[test]
    fn message() {
        let now = Utc::now();
        let topic = Topic::new("test");
        let part = Partition { topic, index: 10 };
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);

        assert_eq!(message.partition.topic.as_str(), "test");
        assert_eq!(message.partition.index, 10);
        assert_eq!(message.offset, 10);
        assert_eq!(message.payload, "payload");
        assert_eq!(message.timestamp, now);
    }

    #[test]
    fn fmt_display() {
        let now = Utc::now();
        let part = Partition {
            topic: Topic::new("test"),
            index: 10,
        };
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);

        assert_eq!(
            message.to_string(),
            "BrokerMessage(partition=Partition(10 topic=Topic(test)) offset=10)"
        )
    }
}
