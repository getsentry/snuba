use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

use crate::processing::metrics_buffer::MetricsBuffer;
use crate::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    SubmitError,
};
use crate::types::Message;

#[derive(Clone, Debug)]
pub enum RunTaskError {
    RetryableError,
    InvalidMessage(InvalidMessage),
}

pub type RunTaskFunc<TTransformed> =
    Pin<Box<dyn Future<Output = Result<Message<TTransformed>, RunTaskError>> + Send>>;

pub trait TaskRunner<TPayload, TTransformed>: Send + Sync {
    fn get_task(&self, message: Message<TPayload>) -> RunTaskFunc<TTransformed>;
}

/// This is configuration for the [`RunTaskInThreads`] strategy.
///
/// It defines the runtime on which tasks are being spawned, and the number of
/// concurrently running tasks.
pub struct ConcurrencyConfig {
    /// The configured number of concurrently running tasks.
    pub concurrency: usize,
    runtime: RuntimeOrHandle,
}

impl ConcurrencyConfig {
    /// Creates a new [`ConcurrencyConfig`], spawning a new [`Runtime`].
    ///
    /// The runtime will use the number of worker threads given by the `concurrency`,
    /// and also limit the number of concurrently running tasks as well.
    pub fn new(concurrency: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(concurrency)
            .enable_all()
            .build()
            .unwrap();
        Self {
            concurrency,
            runtime: RuntimeOrHandle::Runtime(runtime),
        }
    }

    /// Creates a new [`ConcurrencyConfig`], reusing an existing [`Runtime`] via
    /// its [`Handle`].
    pub fn with_runtime(concurrency: usize, runtime: Handle) -> Self {
        Self {
            concurrency,
            runtime: RuntimeOrHandle::Handle(runtime),
        }
    }

    /// Returns a [`Handle`] to the underlying runtime.
    pub fn handle(&self) -> Handle {
        match &self.runtime {
            RuntimeOrHandle::Handle(handle) => handle.clone(),
            RuntimeOrHandle::Runtime(runtime) => runtime.handle().to_owned(),
        }
    }
}

enum RuntimeOrHandle {
    Handle(Handle),
    Runtime(Runtime),
}

pub struct RunTaskInThreads<TPayload, TTransformed> {
    next_step: Box<dyn ProcessingStrategy<TTransformed>>,
    task_runner: Box<dyn TaskRunner<TPayload, TTransformed>>,
    concurrency: usize,
    runtime: Handle,
    handles: VecDeque<JoinHandle<Result<Message<TTransformed>, RunTaskError>>>,
    message_carried_over: Option<Message<TTransformed>>,
    commit_request_carried_over: Option<CommitRequest>,
    metrics_buffer: MetricsBuffer,
    metric_name: String,
}

impl<TPayload, TTransformed> RunTaskInThreads<TPayload, TTransformed> {
    pub fn new<N>(
        next_step: N,
        task_runner: Box<dyn TaskRunner<TPayload, TTransformed>>,
        concurrency: &ConcurrencyConfig,
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
            concurrency: concurrency.concurrency,
            runtime: concurrency.handle(),
            handles: VecDeque::new(),
            message_carried_over: None,
            commit_request_carried_over: None,
            metrics_buffer: MetricsBuffer::new(),
            metric_name: format!("arroyo.strategies.{strategy_name}.threads"),
        }
    }
}

impl<TPayload, TTransformed: Send + Sync + 'static> ProcessingStrategy<TPayload>
    for RunTaskInThreads<TPayload, TTransformed>
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
                        Ok(Err(RunTaskError::InvalidMessage(e))) => {
                            return Err(e);
                        }
                        Ok(Err(RunTaskError::RetryableError)) => {
                            tracing::error!("retryable error");
                        }
                        Err(error) => {
                            let error: &dyn std::error::Error = &error;
                            tracing::error!(error, "the thread crashed");
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

        let task = self.task_runner.get_task(message);
        let handle = self.runtime.spawn(task);
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
                    tracing::warn!(
                        %self.metric_name,
                        "Timeout reached while waiting for tasks to finish",
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

        let remaining = timeout.map(|t| t.checked_sub(start.elapsed()).unwrap_or(Duration::ZERO));

        let next_commit = self.next_step.join(remaining)?;

        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{BrokerMessage, Partition, Topic};

    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    struct IdentityTaskRunner {}

    impl<T: Send + Sync + 'static> TaskRunner<T, T> for IdentityTaskRunner {
        fn get_task(&self, message: Message<T>) -> RunTaskFunc<T> {
            Box::pin(async move { Ok(message) })
        }
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    struct Counts {
        submit: u8,
        polled: bool,
    }

    struct Mock(Arc<Mutex<Counts>>);

    impl Mock {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Default::default())))
        }

        fn counts(&self) -> Arc<Mutex<Counts>> {
            self.0.clone()
        }
    }

    impl ProcessingStrategy<String> for Mock {
        fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
            self.0.lock().unwrap().polled = true;
            Ok(None)
        }
        fn submit(&mut self, _message: Message<String>) -> Result<(), SubmitError<String>> {
            self.0.lock().unwrap().submit += 1;
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

    #[test]
    fn test() {
        let concurrency = ConcurrencyConfig::new(1);
        let mut strategy = RunTaskInThreads::new(
            Mock::new(),
            Box::new(IdentityTaskRunner {}),
            &concurrency,
            None,
        );

        let message = Message::new_any_message("hello_world".to_string(), BTreeMap::new());

        strategy.submit(message).unwrap();
        let _ = strategy.poll();
        let _ = strategy.join(None);
    }

    #[test]
    fn test_run_task_in_threads() {
        for poll_after_msg in [false, true] {
            for poll_before_join in [false, true] {
                let next_step = Mock::new();
                let counts = next_step.counts();
                let concurrency = ConcurrencyConfig::new(2);
                let mut strategy = RunTaskInThreads::new(
                    next_step,
                    Box::new(IdentityTaskRunner {}),
                    &concurrency,
                    None,
                );

                let partition = Partition::new(Topic::new("topic"), 0);

                strategy
                    .submit(Message::new_broker_message(
                        "hello".to_string(),
                        partition,
                        0,
                        chrono::Utc::now(),
                    ))
                    .unwrap();

                if poll_after_msg {
                    strategy.poll().unwrap();
                }

                strategy
                    .submit(Message::new_broker_message(
                        "world".to_string(),
                        partition,
                        1,
                        chrono::Utc::now(),
                    ))
                    .unwrap();

                if poll_after_msg {
                    strategy.poll().unwrap();
                }

                if poll_before_join {
                    for _ in 0..10 {
                        if counts.lock().unwrap().submit < 2 {
                            strategy.poll().unwrap();
                            std::thread::sleep(Duration::from_millis(100));
                        } else {
                            break;
                        }
                    }

                    let counts = counts.lock().unwrap();
                    assert_eq!(counts.submit, 2);
                    assert!(counts.polled);
                }

                strategy.join(None).unwrap();

                let counts = counts.lock().unwrap();
                assert_eq!(counts.submit, 2);
                assert!(counts.polled);
            }
        }
    }
}
