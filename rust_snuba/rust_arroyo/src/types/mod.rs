use std::any::type_name;
use std::cmp::Eq;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;

#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Topic(&'static str);

impl Topic {
    pub fn new(name: &str) -> Self {
        static INTERNED_TOPICS: Lazy<Mutex<HashSet<String>>> = Lazy::new(Default::default);
        let mut interner = INTERNED_TOPICS.lock().unwrap();
        interner.insert(name.into());
        let interned_name = interner.get(name).unwrap();

        // SAFETY:
        // - The interner is static, append-only, and only defined within this function.
        // - We insert heap-allocated `String`s that do not move.
        let interned_name = unsafe { std::mem::transmute::<&str, &'static str>(interned_name) };
        Self(interned_name)
    }

    pub fn as_str(&self) -> &str {
        self.0
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicOrPartition {
    Topic(Topic),
    Partition(Partition),
}

impl From<Topic> for TopicOrPartition {
    fn from(value: Topic) -> Self {
        Self::Topic(value)
    }
}

impl From<Partition> for TopicOrPartition {
    fn from(value: Partition) -> Self {
        Self::Partition(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerMessage<T> {
    pub payload: T,
    pub partition: Partition,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

impl<T> BrokerMessage<T> {
    pub fn new(payload: T, partition: Partition, offset: u64, timestamp: DateTime<Utc>) -> Self {
        Self {
            payload,
            partition,
            offset,
            timestamp,
        }
    }

    pub fn replace<TReplaced>(self, replacement: TReplaced) -> BrokerMessage<TReplaced> {
        BrokerMessage {
            payload: replacement,
            partition: self.partition,
            offset: self.offset,
            timestamp: self.timestamp,
        }
    }

    /// Map a fallible function over this messages's payload.
    pub fn try_map<TReplaced, E, F: FnOnce(T) -> Result<TReplaced, E>>(
        self,
        f: F,
    ) -> Result<BrokerMessage<TReplaced>, E> {
        let Self {
            payload,
            partition,
            offset,
            timestamp,
        } = self;

        let payload = f(payload)?;

        Ok(BrokerMessage {
            payload,
            partition,
            offset,
            timestamp,
        })
    }
}

impl<T> fmt::Display for BrokerMessage<T> {
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

impl<T> AnyMessage<T> {
    pub fn new(payload: T, committable: BTreeMap<Partition, u64>) -> Self {
        Self {
            payload,
            committable,
        }
    }

    pub fn replace<TReplaced>(self, replacement: TReplaced) -> AnyMessage<TReplaced> {
        AnyMessage {
            payload: replacement,
            committable: self.committable,
        }
    }

    /// Map a fallible function over this messages's payload.
    pub fn try_map<TReplaced, E, F: FnOnce(T) -> Result<TReplaced, E>>(
        self,
        f: F,
    ) -> Result<AnyMessage<TReplaced>, E> {
        let Self {
            payload,
            committable,
        } = self;

        let payload = f(payload)?;

        Ok(AnyMessage {
            payload,
            committable,
        })
    }
}

impl<T> fmt::Display for AnyMessage<T> {
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

impl<T> Message<T> {
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

    pub fn payload(&self) -> &T {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage { payload, .. }) => payload,
            InnerMessage::AnyMessage(AnyMessage { payload, .. }) => payload,
        }
    }

    /// Consumes the message and returns its payload.
    pub fn into_payload(self) -> T {
        match self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage { payload, .. }) => payload,
            InnerMessage::AnyMessage(AnyMessage { payload, .. }) => payload,
        }
    }

    /// Returns an iterator over this message's committable offsets.
    pub fn committable(&self) -> Committable {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage {
                partition, offset, ..
            }) => Committable(CommittableInner::Broker(std::iter::once((
                *partition,
                offset + 1,
            )))),
            InnerMessage::AnyMessage(AnyMessage { committable, .. }) => {
                Committable(CommittableInner::Any(committable.iter()))
            }
        }
    }

    pub fn replace<TReplaced>(self, replacement: TReplaced) -> Message<TReplaced> {
        match self.inner_message {
            InnerMessage::BrokerMessage(inner) => Message {
                inner_message: InnerMessage::BrokerMessage(inner.replace(replacement)),
            },
            InnerMessage::AnyMessage(inner) => Message {
                inner_message: InnerMessage::AnyMessage(inner.replace(replacement)),
            },
        }
    }

    /// Map a fallible function over this messages's payload.
    pub fn try_map<TReplaced, E, F: Fn(T) -> Result<TReplaced, E>>(
        self,
        f: F,
    ) -> Result<Message<TReplaced>, E> {
        match self.inner_message {
            InnerMessage::BrokerMessage(inner) => {
                let inner = inner.try_map(f)?;
                Ok(inner.into())
            }
            InnerMessage::AnyMessage(inner) => {
                let inner = inner.try_map(f)?;
                Ok(inner.into())
            }
        }
    }

    // Returns this message's timestamp, if it has one.
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match &self.inner_message {
            InnerMessage::BrokerMessage(m) => Some(m.timestamp),
            InnerMessage::AnyMessage(_) => None,
        }
    }
}

impl<T> fmt::Display for Message<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage {
                partition, offset, ..
            }) => {
                write!(
                    f,
                    "Message<{}>(partition={partition}), offset={offset}",
                    type_name::<T>(),
                )
            }
            InnerMessage::AnyMessage(AnyMessage { committable, .. }) => {
                write!(
                    f,
                    "Message<{}>(committable={committable:?})",
                    type_name::<T>(),
                )
            }
        }
    }
}

impl<T> From<BrokerMessage<T>> for Message<T> {
    fn from(value: BrokerMessage<T>) -> Self {
        Self {
            inner_message: InnerMessage::BrokerMessage(value),
        }
    }
}

impl<T> From<AnyMessage<T>> for Message<T> {
    fn from(value: AnyMessage<T>) -> Self {
        Self {
            inner_message: InnerMessage::AnyMessage(value),
        }
    }
}

#[derive(Debug, Clone)]
enum CommittableInner<'a> {
    Any(std::collections::btree_map::Iter<'a, Partition, u64>),
    Broker(std::iter::Once<(Partition, u64)>),
}

/// An iterator over a `Message`'s committable offsets.
///
/// This is produced by [`Message::committable`].
#[derive(Debug, Clone)]
pub struct Committable<'a>(CommittableInner<'a>);

impl<'a> Iterator for Committable<'a> {
    type Item = (Partition, u64);

    fn next(&mut self) -> Option<Self::Item> {
        match self.0 {
            CommittableInner::Any(ref mut inner) => inner.next().map(|(k, v)| (*k, *v)),
            CommittableInner::Broker(ref mut inner) => inner.next(),
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
