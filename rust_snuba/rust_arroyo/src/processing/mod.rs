use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard};
use thiserror::Error;

use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::kafka::KafkaConsumer;
use crate::backends::{AssignmentCallbacks, CommitOffsets, Consumer, ConsumerError};
use crate::processing::dlq::{BufferedMessages, DlqPolicy, DlqPolicyWrapper};
use crate::processing::strategies::{MessageRejected, SubmitError};
use crate::types::{InnerMessage, Message, Partition, Topic};
use crate::utils::timing::Deadline;
use crate::{counter, timer};

pub mod dlq;
mod metrics_buffer;
pub mod strategies;

use strategies::{ProcessingStrategy, ProcessingStrategyFactory};

#[derive(Debug, Clone)]
pub struct InvalidState;

#[derive(Debug, Clone)]
pub struct PollError;

#[derive(Debug, Clone)]
pub struct PauseError;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("invalid state")]
    InvalidState,
    #[error("poll error")]
    Poll(#[source] ConsumerError),
    #[error("pause error")]
    Pause(#[source] ConsumerError),
    #[error("strategy panicked")]
    StrategyPanic,
}

const BACKPRESSURE_THRESHOLD: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct ConsumerState<TPayload>(Arc<(AtomicBool, Mutex<ConsumerStateInner<TPayload>>)>);

struct ConsumerStateInner<TPayload> {
    processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    strategy: Option<Box<dyn ProcessingStrategy<TPayload>>>,
    backpressure_deadline: Option<Deadline>,
    metrics_buffer: metrics_buffer::MetricsBuffer,
    dlq_policy: DlqPolicyWrapper<TPayload>,
}

impl<TPayload: Send + Sync + 'static> ConsumerState<TPayload> {
    pub fn new(
        processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
        dlq_policy: Option<DlqPolicy<TPayload>>,
    ) -> Self {
        let inner = ConsumerStateInner {
            processing_factory,
            strategy: None,
            backpressure_deadline: None,
            metrics_buffer: metrics_buffer::MetricsBuffer::new(),
            dlq_policy: DlqPolicyWrapper::new(dlq_policy),
        };
        Self(Arc::new((AtomicBool::new(false), Mutex::new(inner))))
    }

    fn is_paused(&self) -> bool {
        self.0 .0.load(Ordering::Relaxed)
    }

    fn set_paused(&self, paused: bool) {
        self.0 .0.store(paused, Ordering::Relaxed)
    }

    fn locked_state(&self) -> MutexGuard<ConsumerStateInner<TPayload>> {
        self.0 .1.lock()
    }
}

impl<T> ConsumerStateInner<T> {
    fn clear_backpressure(&mut self) {
        if let Some(deadline) = self.backpressure_deadline.take() {
            self.metrics_buffer
                .incr_timing("arroyo.consumer.backpressure.time", deadline.elapsed());
        }
    }
}

pub struct Callbacks<TPayload>(pub ConsumerState<TPayload>);

#[derive(Debug, Clone)]
pub struct ProcessorHandle {
    shutdown_requested: Arc<AtomicBool>,
}

impl ProcessorHandle {
    pub fn signal_shutdown(&mut self) {
        self.shutdown_requested.store(true, Ordering::Relaxed);
    }
}

impl<TPayload: Send + Sync + 'static> AssignmentCallbacks for Callbacks<TPayload> {
    // TODO: Having the initialization of the strategy here
    // means that ProcessingStrategy and ProcessingStrategyFactory
    // have to be Send and Sync, which is really limiting and unnecessary.
    // Revisit this so that it is not the callback that perform the
    // initialization.  But we just provide a signal back to the
    // processor to do that.
    fn on_assign(&self, partitions: HashMap<Partition, u64>) {
        counter!(
            "arroyo.consumer.partitions_assigned.count",
            partitions.len() as i64
        );

        let start = Instant::now();

        let mut state = self.0.locked_state();
        state.processing_factory.update_partitions(&partitions);
        state.strategy = Some(state.processing_factory.create());
        state.dlq_policy.reset_dlq_limits(&partitions);

        timer!("arroyo.consumer.create_strategy.time", start.elapsed());
    }

    fn on_revoke<C: CommitOffsets>(&self, commit_offsets: C, partitions: Vec<Partition>) {
        tracing::info!("Start revoke partitions");
        counter!(
            "arroyo.consumer.partitions_revoked.count",
            partitions.len() as i64,
        );

        let start = Instant::now();

        let mut state = self.0.locked_state();
        if let Some(s) = state.strategy.as_mut() {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                s.close();
                s.join(None)
            }));

            match result {
                Ok(join_result) => {
                    if let Ok(Some(commit_request)) = join_result {
                        state.dlq_policy.flush(&commit_request.positions);
                        tracing::info!("Committing offsets");
                        let res = commit_offsets.commit(commit_request.positions);

                        if let Err(err) = res {
                            let error: &dyn std::error::Error = &err;
                            tracing::error!(error, "Failed to commit offsets");
                        }
                    }
                }

                Err(err) => {
                    tracing::error!(?err, "Strategy panicked during close/join");
                }
            }
        }
        state.strategy = None;
        self.0.set_paused(false);
        state.clear_backpressure();

        timer!("arroyo.consumer.join.time", start.elapsed());

        tracing::info!("End revoke partitions");

        // TODO: Figure out how to flush the metrics buffer from the recovation callback.
    }
}

/// A stream processor manages the relationship between a ``Consumer``
/// instance and a ``ProcessingStrategy``, ensuring that processing
/// strategies are instantiated on partition assignment and closed on
/// partition revocation.
pub struct StreamProcessor<TPayload: Clone> {
    consumer: Box<dyn Consumer<TPayload, Callbacks<TPayload>>>,
    consumer_state: ConsumerState<TPayload>,
    message: Option<Message<TPayload>>,
    processor_handle: ProcessorHandle,
    buffered_messages: BufferedMessages<TPayload>,
    metrics_buffer: metrics_buffer::MetricsBuffer,
}

impl StreamProcessor<KafkaPayload> {
    pub fn with_kafka<F: ProcessingStrategyFactory<KafkaPayload> + 'static>(
        config: KafkaConfig,
        factory: F,
        topic: Topic,
        dlq_policy: Option<DlqPolicy<KafkaPayload>>,
    ) -> Self {
        let consumer_state = ConsumerState::new(Box::new(factory), dlq_policy);
        let callbacks = Callbacks(consumer_state.clone());

        // TODO: Can this fail?
        let consumer = Box::new(KafkaConsumer::new(config, &[topic], callbacks).unwrap());

        Self::new(consumer, consumer_state)
    }
}

impl<TPayload: Clone + Send + Sync + 'static> StreamProcessor<TPayload> {
    pub fn new(
        consumer: Box<dyn Consumer<TPayload, Callbacks<TPayload>>>,
        consumer_state: ConsumerState<TPayload>,
    ) -> Self {
        let max_buffered_messages_per_partition = consumer_state
            .locked_state()
            .dlq_policy
            .max_buffered_messages_per_partition();

        Self {
            consumer,
            consumer_state,
            message: None,
            processor_handle: ProcessorHandle {
                shutdown_requested: Arc::new(AtomicBool::new(false)),
            },
            buffered_messages: BufferedMessages::new(max_buffered_messages_per_partition),
            metrics_buffer: metrics_buffer::MetricsBuffer::new(),
        }
    }

    pub fn run_once(&mut self) -> Result<(), RunError> {
        // In case the strategy panics, we attempt to catch it and return an error.
        // This enables the consumer to crash rather than hang indedinitely.
        panic::catch_unwind(AssertUnwindSafe(|| self._run_once()))
            .unwrap_or(Err(RunError::StrategyPanic))
    }

    fn _run_once(&mut self) -> Result<(), RunError> {
        counter!("arroyo.consumer.run.count");

        let consumer_is_paused = self.consumer_state.is_paused();
        if consumer_is_paused {
            // If the consumer was paused, it should not be returning any messages
            // on `poll`.
            let res = self.consumer.poll(Some(Duration::ZERO)).unwrap();
            if res.is_some() {
                return Err(RunError::InvalidState);
            }
        } else if self.message.is_none() {
            // Otherwise, we need to try fetch a new message from the consumer,
            // even if there is no active assignment and/or processing strategy.
            let poll_start = Instant::now();
            //TODO: Support errors properly
            match self.consumer.poll(Some(Duration::from_secs(1))) {
                Ok(msg) => {
                    self.metrics_buffer
                        .incr_timing("arroyo.consumer.poll.time", poll_start.elapsed());

                    if let Some(broker_msg) = msg {
                        self.message = Some(Message {
                            inner_message: InnerMessage::BrokerMessage(broker_msg.clone()),
                        });

                        self.buffered_messages.append(broker_msg);
                    }
                }
                Err(err) => {
                    let error: &dyn std::error::Error = &err;
                    tracing::error!(error, "poll error");
                    return Err(RunError::Poll(err));
                }
            }
        }

        // since we do not drive the kafka consumer at this point, it is safe to acquire the state
        // lock, as we can be sure that for the rest of this function, no assignment callback will
        // run.
        let mut consumer_state = self.consumer_state.locked_state();
        let consumer_state: &mut ConsumerStateInner<_> = &mut consumer_state;

        let Some(strategy) = consumer_state.strategy.as_mut() else {
            match self.message.as_ref() {
                None => return Ok(()),
                Some(_) => return Err(RunError::InvalidState),
            }
        };
        let processing_start = Instant::now();

        match strategy.poll() {
            Ok(None) => {}
            Ok(Some(request)) => {
                for (partition, offset) in &request.positions {
                    self.buffered_messages.pop(partition, offset - 1);
                }

                consumer_state.dlq_policy.flush(&request.positions);
                self.consumer.commit_offsets(request.positions).unwrap();
            }
            Err(strategies::PollError::InvalidMessage(e)) => {
                match self.buffered_messages.pop(&e.partition, e.offset) {
                    Some(msg) => {
                        tracing::error!(?e, "Invalid message");
                        consumer_state.dlq_policy.produce(msg);
                    }
                    None => {
                        tracing::error!("Could not find invalid message in buffer");
                    }
                }
            }

            Err(strategies::PollError::JoinError(error)) => {
                let error: &dyn std::error::Error = &error;
                tracing::error!(error, "the thread crashed");
            }

            Err(strategies::PollError::Other(error)) => {
                tracing::error!(error, "the thread errored");
            }
        };

        let Some(msg_s) = self.message.take() else {
            self.metrics_buffer.incr_timing(
                "arroyo.consumer.processing.time",
                processing_start.elapsed(),
            );
            return Ok(());
        };

        let ret = strategy.submit(msg_s);
        self.metrics_buffer.incr_timing(
            "arroyo.consumer.processing.time",
            processing_start.elapsed(),
        );

        match ret {
            Ok(()) => {
                // Resume if we are currently in a paused state
                if consumer_is_paused {
                    let partitions = self.consumer.tell().unwrap().into_keys().collect();

                    match self.consumer.resume(partitions) {
                        Ok(()) => {
                            self.consumer_state.set_paused(false);
                        }
                        Err(err) => {
                            let error: &dyn std::error::Error = &err;
                            tracing::error!(error, "pause error");
                            return Err(RunError::Pause(err));
                        }
                    }
                }

                // Clear backpressure timestamp if it is set
                consumer_state.clear_backpressure();
            }
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                // Put back the carried over message
                self.message = Some(message);

                let Some(deadline) = consumer_state.backpressure_deadline else {
                    consumer_state.backpressure_deadline =
                        Some(Deadline::new(BACKPRESSURE_THRESHOLD));
                    return Ok(());
                };

                // If we are in the backpressure state for more than 1 second,
                // we pause the consumer and hold the message until it is
                // accepted, at which point we can resume consuming.
                if !consumer_is_paused && deadline.has_elapsed() {
                    tracing::warn!(
                        "Consumer is in backpressure state for more than 1 second, pausing",
                    );

                    let partitions = self.consumer.tell().unwrap().into_keys().collect();

                    match self.consumer.pause(partitions) {
                        Ok(()) => {
                            self.consumer_state.set_paused(true);
                        }
                        Err(err) => {
                            let error: &dyn std::error::Error = &err;
                            tracing::error!(error, "pause error");
                            return Err(RunError::Pause(err));
                        }
                    }
                }
            }
            Err(SubmitError::InvalidMessage(message)) => {
                let invalid_message = self
                    .buffered_messages
                    .pop(&message.partition, message.offset);

                if let Some(msg) = invalid_message {
                    tracing::error!(?message, "Invalid message");
                    consumer_state.dlq_policy.produce(msg);
                } else {
                    tracing::error!(?message, "Could not retrieve invalid message from buffer");
                }
            }
        }
        Ok(())
    }

    /// The main run loop, see class docstring for more information.
    pub fn run(mut self) -> Result<(), RunError> {
        while !self
            .processor_handle
            .shutdown_requested
            .load(Ordering::Relaxed)
        {
            if let Err(e) = self.run_once() {
                let mut trait_callbacks = self.consumer_state.locked_state();

                if let Some(strategy) = trait_callbacks.strategy.as_mut() {
                    strategy.terminate();
                }

                drop(trait_callbacks); // unlock mutex so we can close consumer
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn get_handle(&self) -> ProcessorHandle {
        self.processor_handle.clone()
    }

    pub fn tell(&self) -> HashMap<Partition, u64> {
        self.consumer.tell().unwrap()
    }

    pub fn shutdown(self) {}
}

#[cfg(test)]
mod tests {
    use super::strategies::{
        CommitRequest, ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
    };
    use super::*;
    use crate::backends::local::broker::LocalBroker;
    use crate::backends::local::LocalConsumer;
    use crate::backends::storages::memory::MemoryMessageStorage;
    use crate::types::{Message, Partition, Topic};
    use crate::utils::clock::SystemClock;
    use std::collections::HashMap;
    use std::time::Duration;
    use uuid::Uuid;

    struct TestStrategy {
        message: Option<Message<String>>,
    }
    impl ProcessingStrategy<String> for TestStrategy {
        #[allow(clippy::manual_map)]
        fn poll(&mut self) -> Result<Option<CommitRequest>, strategies::PollError> {
            Ok(self.message.as_ref().map(|message| CommitRequest {
                positions: HashMap::from_iter(message.committable()),
            }))
        }

        fn submit(&mut self, message: Message<String>) -> Result<(), SubmitError<String>> {
            self.message = Some(message);
            Ok(())
        }

        fn close(&mut self) {}

        fn terminate(&mut self) {}

        fn join(
            &mut self,
            _: Option<Duration>,
        ) -> Result<Option<CommitRequest>, strategies::PollError> {
            Ok(None)
        }
    }

    struct TestFactory {}
    impl ProcessingStrategyFactory<String> for TestFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<String>> {
            Box::new(TestStrategy { message: None })
        }
    }

    fn build_broker() -> LocalBroker<String> {
        let storage: MemoryMessageStorage<String> = Default::default();
        let clock = SystemClock {};
        let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));

        let topic1 = Topic::new("test1");

        let _ = broker.create_topic(topic1, 1);
        broker
    }

    #[test]
    fn test_processor() {
        let broker = build_broker();

        let consumer_state = ConsumerState::new(Box::new(TestFactory {}), None);

        let consumer = Box::new(LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            false,
            &[Topic::new("test1")],
            Callbacks(consumer_state.clone()),
        ));

        let mut processor = StreamProcessor::new(consumer, consumer_state);
        let res = processor.run_once();
        assert!(res.is_ok())
    }

    #[test]
    fn test_consume() {
        let mut broker = build_broker();
        let topic1 = Topic::new("test1");
        let partition = Partition::new(topic1, 0);
        let _ = broker.produce(&partition, "message1".to_string());
        let _ = broker.produce(&partition, "message2".to_string());

        let consumer_state = ConsumerState::new(Box::new(TestFactory {}), None);

        let consumer = Box::new(LocalConsumer::new(
            Uuid::nil(),
            Arc::new(Mutex::new(broker)),
            "test_group".to_string(),
            false,
            &[Topic::new("test1")],
            Callbacks(consumer_state.clone()),
        ));

        let mut processor = StreamProcessor::new(consumer, consumer_state);
        let res = processor.run_once();
        assert!(res.is_ok());
        let res = processor.run_once();
        assert!(res.is_ok());

        let expected = HashMap::from([(partition, 2)]);

        assert_eq!(processor.tell(), expected)
    }

    #[test]
    fn test_strategy_panic() {
        // Tests that a panic in any of the poll, submit, join, or close methods will crash the consumer
        // and not deadlock
        struct TestStrategy {
            panic_on: &'static str, // poll, submit, join, close
        }
        impl ProcessingStrategy<String> for TestStrategy {
            fn poll(&mut self) -> Result<Option<CommitRequest>, strategies::PollError> {
                if self.panic_on == "poll" {
                    panic!("panic in poll");
                }
                Ok(None)
            }

            fn submit(&mut self, _message: Message<String>) -> Result<(), SubmitError<String>> {
                if self.panic_on == "submit" {
                    panic!("panic in submit");
                }

                Ok(())
            }

            fn close(&mut self) {
                if self.panic_on == "close" {
                    panic!("panic in close");
                }
            }

            fn terminate(&mut self) {}

            fn join(
                &mut self,
                _: Option<Duration>,
            ) -> Result<Option<CommitRequest>, strategies::PollError> {
                if self.panic_on == "join" {
                    panic!("panic in join");
                }

                Ok(None)
            }
        }

        struct TestFactory {
            panic_on: &'static str,
        }
        impl ProcessingStrategyFactory<String> for TestFactory {
            fn create(&self) -> Box<dyn ProcessingStrategy<String>> {
                Box::new(TestStrategy {
                    panic_on: self.panic_on,
                })
            }
        }

        fn build_processor(
            broker: LocalBroker<String>,
            panic_on: &'static str,
        ) -> StreamProcessor<String> {
            let consumer_state = ConsumerState::new(Box::new(TestFactory { panic_on }), None);

            let consumer = Box::new(LocalConsumer::new(
                Uuid::nil(),
                Arc::new(Mutex::new(broker)),
                "test_group".to_string(),
                false,
                &[Topic::new("test1")],
                Callbacks(consumer_state.clone()),
            ));

            StreamProcessor::new(consumer, consumer_state)
        }

        let topic1 = Topic::new("test1");
        let partition = Partition::new(topic1, 0);

        let test_cases = ["poll", "submit", "join", "close"];

        for test_case in test_cases {
            let mut broker = build_broker();
            let _ = broker.produce(&partition, "message1".to_string());
            let _ = broker.produce(&partition, "message2".to_string());
            let mut processor = build_processor(broker, test_case);

            let res = processor.run_once();

            if test_case == "join" || test_case == "close" {
                assert!(res.is_ok());
            } else {
                assert!(res.is_err());
            }

            processor.shutdown();
        }
    }
}
