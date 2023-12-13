pub mod dlq;
mod metrics_buffer;
pub mod strategies;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::kafka::KafkaConsumer;
use crate::backends::{AssignmentCallbacks, CommitOffsets, Consumer, ConsumerError};
use crate::processing::dlq::{BufferedMessages, DlqPolicy, DlqPolicyWrapper};
use crate::processing::strategies::{MessageRejected, SubmitError};
use crate::types::{InnerMessage, Message, Partition, Topic};
use crate::utils::metrics::{get_metrics, Metrics};
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
}

pub struct ConsumerState<TPayload> {
    processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    strategy: Option<Box<dyn ProcessingStrategy<TPayload>>>,
    backpressure_timestamp: Option<Instant>,
    is_paused: bool,
    metrics_buffer: metrics_buffer::MetricsBuffer,
    dlq_policy: DlqPolicyWrapper<TPayload>,
}

impl<TPayload: Send + Sync + 'static> ConsumerState<TPayload> {
    pub fn new(
        processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
        dlq_policy: Option<DlqPolicy<TPayload>>,
    ) -> Self {
        Self {
            processing_factory,
            strategy: None,
            backpressure_timestamp: None,
            is_paused: false,
            metrics_buffer: metrics_buffer::MetricsBuffer::new(),
            dlq_policy: DlqPolicyWrapper::new(dlq_policy),
        }
    }

    fn clear_backpressure(&mut self) {
        if self.backpressure_timestamp.is_some() {
            self.metrics_buffer.incr_timing(
                "arroyo.consumer.backpressure.time",
                self.backpressure_timestamp.unwrap().elapsed(),
            );
            self.backpressure_timestamp = None;
        }
    }
}

pub struct Callbacks<TPayload>(pub Arc<Mutex<ConsumerState<TPayload>>>);

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
        let metrics = get_metrics();
        metrics.increment(
            "arroyo.consumer.partitions_assigned.count",
            partitions.len() as i64,
            None,
        );

        let start = Instant::now();

        let mut state = self.0.lock().unwrap();
        state.strategy = Some(state.processing_factory.create());
        state.dlq_policy.reset_dlq_limits(&partitions);

        metrics.timing(
            "arroyo.consumer.create_strategy.time",
            start.elapsed().as_millis() as u64,
            None,
        );
    }

    fn on_revoke<C: CommitOffsets>(&self, commit_offsets: C, partitions: Vec<Partition>) {
        tracing::info!("Start revoke partitions");
        let metrics = get_metrics();
        metrics.increment(
            "arroyo.consumer.partitions_revoked.count",
            partitions.len() as i64,
            None,
        );

        let start = Instant::now();

        let mut state = self.0.lock().unwrap();
        if let Some(s) = state.strategy.as_mut() {
            s.close();
            if let Ok(Some(commit_request)) = s.join(None) {
                state.dlq_policy.flush(&commit_request.positions);
                tracing::info!("Committing offsets");
                let res = commit_offsets.commit(commit_request.positions);

                if let Err(err) = res {
                    let error: &dyn std::error::Error = &err;
                    tracing::error!(error, "Failed to commit offsets");
                }
            }
        }
        state.strategy = None;
        state.is_paused = false;
        state.clear_backpressure();

        metrics.timing(
            "arroyo.consumer.join.time",
            start.elapsed().as_millis() as u64,
            None,
        );

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
    consumer_state: Arc<Mutex<ConsumerState<TPayload>>>,
    message: Option<Message<TPayload>>,
    processor_handle: ProcessorHandle,
    metrics_buffer: metrics_buffer::MetricsBuffer,
    buffered_messages: BufferedMessages<TPayload>,
}

impl StreamProcessor<KafkaPayload> {
    pub fn with_kafka<F: ProcessingStrategyFactory<KafkaPayload> + 'static>(
        config: KafkaConfig,
        factory: F,
        topic: Topic,
        dlq_policy: Option<DlqPolicy<KafkaPayload>>,
    ) -> Self {
        let consumer_state = Arc::new(Mutex::new(ConsumerState::new(
            Box::new(factory),
            dlq_policy,
        )));
        let callbacks = Callbacks(consumer_state.clone());

        // TODO: Can this fail?
        let consumer = Box::new(KafkaConsumer::new(config, &[topic], callbacks).unwrap());

        Self::new(consumer, consumer_state)
    }
}

impl<TPayload: Clone + Send + Sync + 'static> StreamProcessor<TPayload> {
    pub fn new(
        consumer: Box<dyn Consumer<TPayload, Callbacks<TPayload>>>,
        consumer_state: Arc<Mutex<ConsumerState<TPayload>>>,
    ) -> Self {
        Self {
            consumer,
            consumer_state,
            message: None,
            processor_handle: ProcessorHandle {
                shutdown_requested: Arc::new(AtomicBool::new(false)),
            },
            metrics_buffer: metrics_buffer::MetricsBuffer::new(),
            buffered_messages: BufferedMessages::new(),
        }
    }

    pub fn run_once(&mut self) -> Result<(), RunError> {
        let metrics = get_metrics();
        metrics.increment("arroyo.consumer.run.count", 1, None);

        if self.consumer_state.lock().unwrap().is_paused {
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
        let mut consumer_state = self.consumer_state.lock().unwrap();
        let consumer_state: &mut ConsumerState<_> = &mut consumer_state;

        let Some(strategy) = consumer_state.strategy.as_mut() else {
            match self.message.as_ref() {
                None => return Ok(()),
                Some(_) => return Err(RunError::InvalidState),
            }
        };
        let poll_start = Instant::now();
        let commit_request = strategy.poll();

        self.metrics_buffer
            .incr_timing("arroyo.consumer.processing.time", poll_start.elapsed());

        match commit_request {
            Ok(None) => {}
            Ok(Some(request)) => {
                for (partition, offset) in &request.positions {
                    self.buffered_messages.pop(partition, offset - 1);
                }

                consumer_state.dlq_policy.flush(&request.positions);
                self.consumer.commit_offsets(request.positions).unwrap();
            }
            Err(e) => match self.buffered_messages.pop(&e.partition, e.offset) {
                Some(msg) => {
                    tracing::error!(?e, "Invalid message");
                    consumer_state.dlq_policy.produce(msg);
                }
                None => {
                    tracing::error!("Could not find invalid message in buffer");
                }
            },
        };

        let Some(msg_s) = self.message.take() else {
            return Ok(());
        };
        let processing_start = Instant::now();
        let ret = strategy.submit(msg_s);
        self.metrics_buffer.incr_timing(
            "arroyo.consumer.processing.time",
            processing_start.elapsed(),
        );

        match ret {
            Ok(()) => {
                // Resume if we are currently in a paused state
                if consumer_state.is_paused {
                    let partitions = self.consumer.tell().unwrap().into_keys().collect();

                    match self.consumer.resume(partitions) {
                        Ok(()) => {
                            consumer_state.is_paused = false;
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

                if consumer_state.backpressure_timestamp.is_none() {
                    consumer_state.backpressure_timestamp = Some(Instant::now());
                }

                // If we are in the backpressure state for more than 1 second,
                // we pause the consumer and hold the message until it is
                // accepted, at which point we can resume consuming.
                if !consumer_state.is_paused && consumer_state.backpressure_timestamp.is_some() {
                    let backpressure_duration =
                        consumer_state.backpressure_timestamp.unwrap().elapsed();

                    if backpressure_duration < Duration::from_secs(1) {
                        return Ok(());
                    }

                    tracing::warn!(
                        "Consumer is in backpressure state for more than 1 second, pausing",
                    );

                    let partitions = self.consumer.tell().unwrap().into_keys().collect();

                    match self.consumer.pause(partitions) {
                        Ok(()) => {
                            consumer_state.is_paused = true;
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
                let mut trait_callbacks = self.consumer_state.lock().unwrap();

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
        CommitRequest, InvalidMessage, ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
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
        fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
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

        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
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

        let consumer_state = Arc::new(Mutex::new(ConsumerState::new(
            Box::new(TestFactory {}),
            None,
        )));

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

        let consumer_state = Arc::new(Mutex::new(ConsumerState::new(
            Box::new(TestFactory {}),
            None,
        )));

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
}
