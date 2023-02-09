use crate::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
};
use crate::types::Message;
use std::time::Duration;

pub struct Transform<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    pub function: fn(TPayload) -> Result<TTransformed, InvalidMessage>,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> ProcessingStrategy<TPayload>
    for Transform<TPayload, TTransformed>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), MessageRejected> {
        // TODO: Handle InvalidMessage
        let transformed = (self.function)(message.payload).unwrap();

        self.next_step.submit(Message {
            partition: message.partition,
            offset: message.offset,
            payload: transformed,
            timestamp: message.timestamp,
        })
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::Transform;
    use crate::processing::strategies::{
        CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    };
    use crate::types::{Message, Partition, Topic};
    use chrono::Utc;
    use std::time::Duration;

    #[test]
    fn test_transform() {
        fn identity(value: String) -> Result<String, InvalidMessage> {
            Ok(value)
        }

        struct Noop {}
        impl ProcessingStrategy<String> for Noop {
            fn poll(&mut self) -> Option<CommitRequest> {
                None
            }
            fn submit(&mut self, _message: Message<String>) -> Result<(), MessageRejected> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
                None
            }
        }

        let mut strategy = Transform {
            function: identity,
            next_step: Box::new(Noop {}),
        };

        let partition = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };

        strategy
            .submit(Message::new(
                partition,
                0,
                "Hello world".to_string(),
                Utc::now(),
            ))
            .unwrap();
    }
}
