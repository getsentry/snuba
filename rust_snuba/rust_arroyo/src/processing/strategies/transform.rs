use crate::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
};
use crate::types::Message;
use std::time::Duration;

pub struct Transform<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    pub function: fn(TPayload) -> Result<TTransformed, InvalidMessage>,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
    pub message_carried_over: Option<Message<TTransformed>>,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync>
    Transform<TPayload, TTransformed>
{
    pub fn new<N>(
        function: fn(TPayload) -> Result<TTransformed, InvalidMessage>,
        next_step: N,
    ) -> Self
    where
        N: ProcessingStrategy<TTransformed> + 'static,
    {
        Self {
            function,
            next_step: Box::new(next_step),
            message_carried_over: None,
        }
    }
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> ProcessingStrategy<TPayload>
    for Transform<TPayload, TTransformed>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        if let Some(message) = self.message_carried_over.take() {
            if let Err(MessageRejected {
                message: transformed_message,
            }) = self.next_step.submit(message)
            {
                self.message_carried_over = Some(transformed_message);
            }
        }

        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), MessageRejected<TPayload>> {
        if self.message_carried_over.is_some() {
            return Err(MessageRejected { message });
        }

        // TODO: Handle InvalidMessage
        let transformed = (self.function)(message.payload()).unwrap();

        if let Err(MessageRejected {
            message: transformed_message,
        }) = self.next_step.submit(message.replace(transformed))
        {
            self.message_carried_over = Some(transformed_message);
        }
        Ok(())
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
    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
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
            fn submit(&mut self, _message: Message<String>) -> Result<(), MessageRejected<String>> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
                None
            }
        }

        let mut strategy = Transform::new(identity, Noop {});

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
            })
            .unwrap();
    }
}
