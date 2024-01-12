use crate::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    StrategyError, SubmitError,
};
use crate::types::Message;
use std::time::Duration;

pub struct RunTask<TPayload, TTransformed> {
    pub function:
        Box<dyn Fn(TPayload) -> Result<TTransformed, InvalidMessage> + Send + Sync + 'static>,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
    pub message_carried_over: Option<Message<TTransformed>>,
    pub commit_request_carried_over: Option<CommitRequest>,
}

impl<TPayload, TTransformed> RunTask<TPayload, TTransformed> {
    pub fn new<N, F>(function: F, next_step: N) -> Self
    where
        N: ProcessingStrategy<TTransformed> + 'static,
        F: Fn(TPayload) -> Result<TTransformed, InvalidMessage> + Send + Sync + 'static,
    {
        Self {
            function: Box::new(function),
            next_step: Box::new(next_step),
            message_carried_over: None,
            commit_request_carried_over: None,
        }
    }
}

impl<TPayload, TTransformed: Send + Sync> ProcessingStrategy<TPayload>
    for RunTask<TPayload, TTransformed>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        match self.next_step.poll() {
            Ok(commit_request) => {
                self.commit_request_carried_over =
                    merge_commit_request(self.commit_request_carried_over.take(), commit_request)
            }
            Err(invalid_message) => return Err(invalid_message),
        }

        if let Some(message) = self.message_carried_over.take() {
            match self.next_step.submit(message) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message.into());
                }
                Ok(_) => {}
            }
        }

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let next_message = message
            .try_map(&self.function)
            .map_err(SubmitError::InvalidMessage)?;

        match self.next_step.submit(next_message) {
            Err(SubmitError::MessageRejected(MessageRejected {
                message: transformed_message,
            })) => {
                self.message_carried_over = Some(transformed_message);
            }
            Err(SubmitError::InvalidMessage(invalid_message)) => {
                return Err(SubmitError::InvalidMessage(invalid_message));
            }
            Ok(_) => {}
        }
        Ok(())
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let next_commit = self.next_step.join(timeout)?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use chrono::Utc;

    #[test]
    fn test_run_task() {
        fn identity(value: String) -> Result<String, InvalidMessage> {
            Ok(value)
        }

        struct Noop {}
        impl ProcessingStrategy<String> for Noop {
            fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
            fn submit(&mut self, _message: Message<String>) -> Result<(), SubmitError<String>> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(
                &mut self,
                _timeout: Option<Duration>,
            ) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
        }

        let mut strategy = RunTask::new(identity, Noop {});

        let partition = Partition::new(Topic::new("test"), 0);

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
