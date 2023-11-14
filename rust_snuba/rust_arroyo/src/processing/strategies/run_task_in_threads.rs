use crate::processing::metrics_buffer::MetricsBuffer;
use crate::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    SubmitError,
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
    commit_request_carried_over: Option<CommitRequest>,
    metrics_buffer: MetricsBuffer,
    metric_name: String,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync>
    RunTaskInThreads<TPayload, TTransformed>
{
    pub fn new<N>(
        next_step: N,
        task_runner: Box<dyn TaskRunner<TPayload, TTransformed>>,
        concurrency: usize,
        // If provided, this name is used for metrics
        custom_strategy_name: Option<&'static str>,
    ) -> Self
    where
        N: ProcessingStrategy<TTransformed> + 'static,
    {
        let strategy_name = custom_strategy_name.unwrap_or("run_task_in_threads");

        RunTaskInThreads {
            next_step: Box::new(next_step),
            task_runner,
            concurrency,
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(concurrency)
                .enable_all()
                .build()
                .unwrap(),
            handles: VecDeque::new(),
            message_carried_over: None,
            commit_request_carried_over: None,
            metrics_buffer: MetricsBuffer::new(),
            metric_name: format!("arroyo.strategies.{strategy_name}.threads"),
        }
    }
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync + 'static>
    ProcessingStrategy<TPayload> for RunTaskInThreads<TPayload, TTransformed>
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        self.metrics_buffer
            .gauge(&self.metric_name, self.handles.len() as u64);

        if let Some(message) = self.message_carried_over.take() {
            match self.next_step.submit(message) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message);
                }
                Ok(_) => {}
            }
        }

        while !self.handles.is_empty() {
            if let Some(front) = self.handles.front() {
                if front.is_finished() {
                    let handle = self.handles.pop_front().unwrap();
                    match self.runtime.block_on(handle) {
                        Ok(Ok(message)) => match self.next_step.submit(message) {
                            Err(SubmitError::MessageRejected(MessageRejected {
                                message: transformed_message,
                            })) => {
                                self.message_carried_over = Some(transformed_message);
                            }
                            Err(SubmitError::InvalidMessage(invalid_message)) => {
                                return Err(invalid_message);
                            }
                            Ok(_) => {}
                        },
                        Ok(Err(e)) => {
                            return Err(e);
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

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        if self.handles.len() > self.concurrency {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
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

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
        let start = Instant::now();

        // Poll until there are no more messages or timeout is hit
        while self.message_carried_over.is_some() || !self.handles.is_empty() {
            if let Some(t) = timeout {
                if start.elapsed() > t {
                    log::warn!(
                        "[{}] Timeout reached while waiting for tasks to finish",
                        self.metric_name
                    );
                    break;
                }
            }

            let commit_request = self.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);
        }

        // Cancel remaining tasks if any
        for handle in &self.handles {
            handle.abort();
        }
        self.handles.clear();
        self.metrics_buffer.flush();

        let remaining = match timeout {
            Some(t) => {
                let elapsed = start.elapsed();
                if elapsed > t {
                    Some(Duration::ZERO)
                } else {
                    Some(t - elapsed)
                }
            }
            None => None,
        };

        let next_commit = self.next_step.join(remaining)?;

        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test() {
        struct Noop {}
        impl ProcessingStrategy<String> for Noop {
            fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
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
            ) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
        }

        struct IdentityTaskRunner {}

        impl<T: Clone + Send + Sync + 'static> TaskRunner<T, T> for IdentityTaskRunner {
            fn get_task(&self, message: Message<T>) -> RunTaskFunc<T> {
                Box::pin(async move { Ok(message) })
            }
        }

        let mut strategy = RunTaskInThreads::new(Noop {}, Box::new(IdentityTaskRunner {}), 1, None);

        let message = Message::new_any_message("hello_world".to_string(), BTreeMap::new());

        strategy.submit(message).unwrap();
        let _ = strategy.poll();
        let _ = strategy.join(None);
    }
}
