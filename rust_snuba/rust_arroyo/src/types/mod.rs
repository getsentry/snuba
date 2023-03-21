use chrono::{DateTime, Utc};
use std::any::type_name;
use std::cmp::Eq;
use std::fmt;
use std::hash::Hash;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Topic {
    pub name: String,
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic({})", self.name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
pub struct Message<T: Clone> {
    pub partition: Partition,
    pub offset: u64,
    pub payload: T,
    pub timestamp: DateTime<Utc>,
}

impl<T: Clone> Message<T> {
    pub fn new(partition: Partition, offset: u64, payload: T, timestamp: DateTime<Utc>) -> Self {
        Self {
            partition,
            offset,
            payload,
            timestamp,
        }
    }
    pub fn next_offset(&self) -> u64 {
        self.offset + 1
    }
}

impl<T: Clone> fmt::Display for Message<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Message<{}>(partition={}), offset={}",
            type_name::<T>(),
            &self.partition,
            &self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, Partition, Topic};
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn message() {
        let now = Utc::now();
        let topic = Topic {
            name: "test".to_string(),
        };
        let part = Partition { topic, index: 10 };
        let message = Message::new(part, 10, "payload".to_string(), now);

        assert_eq!(message.partition.topic.name, "test");
        assert_eq!(message.partition.index, 10);
        assert_eq!(message.offset, 10);
        assert_eq!(message.payload, "payload");
        assert_eq!(message.timestamp, now);
        assert_eq!(message.next_offset(), 11)
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
        let message = Message::new(part, 10, "payload".to_string(), now);

        assert_eq!(
            message.to_string(),
            "Message<alloc::string::String>(partition=Partition(10 topic=Topic(test))), offset=10"
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
        let message = Message::new(part, 10, "payload".to_string(), now);
        let message2 = message.clone();

        assert_eq!(message, message2);
        assert_ne!(
            &message as *const Message<String>,
            &message2 as *const Message<String>
        );
    }
}
