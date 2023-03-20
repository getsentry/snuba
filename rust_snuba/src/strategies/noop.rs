use std::time::Duration;

use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::Message;

pub struct Noop;

impl<T> ProcessingStrategy<T> for Noop
where
    T: Clone,
{
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }
    fn submit(&mut self, _message: Message<T>) -> Result<(), MessageRejected> {
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}
