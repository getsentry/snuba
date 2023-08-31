use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use crate::types::Message;

#[derive(Clone)]
pub struct TestStrategy<T> {
    pub messages: Arc<Mutex<Vec<Message<T>>>>,
}

impl<T> Default for TestStrategy<T> {
    fn default() -> Self {
        TestStrategy {
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T> TestStrategy<T> {
    pub fn new() -> Self {
        TestStrategy::default()
    }
}

impl<T: Send + Clone> ProcessingStrategy<T> for TestStrategy<T> {
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), MessageRejected<T>> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }

    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}
