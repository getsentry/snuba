use async_trait::async_trait;

use crate::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
};
use crate::types::Message;
use std::time::Duration;

pub struct Transform<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    pub function: fn(TPayload) -> Result<TTransformed, InvalidMessage>,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
}

#[async_trait]
impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> ProcessingStrategy<TPayload>
    for Transform<TPayload, TTransformed>
{
    async fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll().await
    }

    async fn submit(&mut self, message: Message<TPayload>) -> Result<(), MessageRejected> {
        // TODO: Handle InvalidMessage
        let transformed = (self.function)(message.payload()).unwrap();

        self.next_step.submit(message.replace(transformed)).await
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    async fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.next_step.join(timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::Transform;
    use crate::processing::strategies::{
        CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    };
    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use chrono::Utc;
    use std::time::Duration;
    use async_trait::async_trait;

    #[tokio::test]
    async fn test_transform() {
        fn identity(value: String) -> Result<String, InvalidMessage> {
            Ok(value)
        }

        struct Noop {}

        #[async_trait]
        impl ProcessingStrategy<String> for Noop {
            async fn poll(&mut self) -> Option<CommitRequest> {
                None
            }
            async fn submit(&mut self, _message: Message<String>) -> Result<(), MessageRejected> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            async fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
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
            .submit(Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    "Hello world".to_string(),
                    partition,
                    0,
                    Utc::now(),
                )),
            }).await.unwrap();
    }
}
