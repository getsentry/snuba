use crate::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
};
use crate::types::Message;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

pub type RunTaskFunc<TTransformed> =
    Pin<Box<dyn Future<Output = Result<Message<TTransformed>, InvalidMessage>> + Send>>;

pub trait TaskRunner<TPayload: Clone, TTransformed: Clone + Send + Sync>: Send + Sync {
    fn get_task(&self, message: Message<TPayload>) -> RunTaskFunc<TTransformed>;
}

pub struct RunTaskInThreads<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    next_step: Box<dyn ProcessingStrategy<TTransformed>>,
    task_runner: Box<dyn TaskRunner<TPayload, TTransformed>>,
    concurrency: usize,
    runtime: tokio::runtime::Runtime,
    handles: VecDeque<JoinHandle<Result<Message<TTransformed>, InvalidMessage>>>,
    message_carried_over: Option<Message<TTransformed>>,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync>
    RunTaskInThreads<TPayload, TTransformed>
{
    pub fn new<N>(
        next_step: N,
        task_runner: Box<dyn TaskRunner<TPayload, TTransformed>>,
        concurrency: usize,
    ) -> Self
    where
        N: ProcessingStrategy<TTransformed> + 'static,
    {
        RunTaskInThreads {
            next_step: Box::new(next_step),
            task_runner,
            concurrency,
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
            handles: VecDeque::new(),
            message_carried_over: None,
        }
    }
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync + 'static>
    ProcessingStrategy<TPayload> for RunTaskInThreads<TPayload, TTransformed>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        if let Some(message) = self.message_carried_over.take() {
            if let Err(MessageRejected {
                message: transformed_message,
            }) = self.next_step.submit(message)
            {
                self.message_carried_over = Some(transformed_message);
                return None;
            }
        }

        while !self.handles.is_empty() {
            if let Some(front) = self.handles.front() {
                if front.is_finished() {
                    let handle = self.handles.pop_front().unwrap();
                    match self.runtime.block_on(handle) {
                        Ok(Ok(message)) => {
                            if let Err(MessageRejected {
                                message: transformed_message,
                            }) = self.next_step.submit(message)
                            {
                                self.message_carried_over = Some(transformed_message);
                            }
                        }
                        Ok(Err(e)) => {
                            log::error!("function errored {:?}", e);
                        }
                        Err(e) => {
                            log::error!("the thread crashed {}", e);
                        }
                    }
                } else {
                    break;
                }
            }
        }

        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), MessageRejected<TPayload>> {
        if self.message_carried_over.is_some() {
            log::warn!("carried over message, rejecting subsequent messages");
            return Err(MessageRejected { message });
        }

        if self.handles.len() > self.concurrency {
            log::warn!("Reached max concurrency, rejecting message");
            return Err(MessageRejected { message });
        }

        let handle = self.runtime.spawn(self.task_runner.get_task(message));
        self.handles.push_back(handle);

        Ok(())
    }

    fn close(&mut self) {
        self.next_step.close();
    }

    fn terminate(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        self.handles.clear();
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        let start = Instant::now();
        let mut remaining: Option<Duration> = timeout;
        let mut commit_request = None;

        // Poll until there are no more messages or timeout is hit
        while self.message_carried_over.is_some() || !self.handles.is_empty() {
            if let Some(t) = remaining {
                remaining = Some(t - start.elapsed());
                if remaining.unwrap() <= Duration::from_secs(0) {
                    log::warn!("Timeout reached while waiting for tasks to finish");
                    break;
                }
            }

            let next_commit = self.poll();
            commit_request = merge_commit_request(commit_request, next_commit);
        }

        // Cancel remaining tasks if any
        for handle in &self.handles {
            handle.abort();
        }
        self.handles.clear();

        let next_commit = self.next_step.join(timeout);
        merge_commit_request(commit_request, next_commit)
    }
}

#[cfg(test)]
mod tests {
    use super::{RunTaskFunc, RunTaskInThreads, TaskRunner};
    use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
    use crate::types::Message;
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn test() {
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

        struct IdentityTaskRunner {}

        impl<T: Clone + Send + Sync + 'static> TaskRunner<T, T> for IdentityTaskRunner {
            fn get_task(&self, message: Message<T>) -> RunTaskFunc<T> {
                Box::pin(async move { Ok(message) })
            }
        }

        let mut strategy = RunTaskInThreads::new(Noop {}, Box::new(IdentityTaskRunner {}), 1);

        let message = Message::new_any_message("hello_world".to_string(), BTreeMap::new());

        strategy.submit(message).unwrap();
        strategy.poll();
        strategy.join(None);
    }
}
