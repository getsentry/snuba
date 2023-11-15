mod dlq;
mod metrics_buffer;
pub mod strategies;

use crate::backends::{AssignmentCallbacks, Consumer};
use crate::processing::strategies::{MessageRejected, SubmitError};
use crate::types::{InnerMessage, Message, Partition, Topic};
use crate::utils::metrics::{get_metrics, Metrics};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use strategies::{ProcessingStrategy, ProcessingStrategyFactory};

#[derive(Debug, Clone)]
pub struct InvalidState;

#[derive(Debug, Clone)]
pub struct PollError;

#[derive(Debug, Clone)]
pub struct PauseError;

#[derive(Debug, Clone)]
pub enum RunError {
    InvalidState,
    PollError,
    PauseError,
}

struct Strategies<TPayload> {
    processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    strategy: Option<Box<dyn ProcessingStrategy<TPayload>>>,
}

struct Callbacks<TPayload> {
    strategies: Arc<Mutex<Strategies<TPayload>>>,
    consumer: Arc<Mutex<dyn Consumer<TPayload>>>,
}

#[derive(Debug, Clone)]
pub struct ProcessorHandle {
    shutdown_requested: Arc<AtomicBool>,
}

impl ProcessorHandle {
    pub fn signal_shutdown(&mut self) {
        self.shutdown_requested.store(true, Ordering::Relaxed);
    }
}

impl<TPayload: 'static> AssignmentCallbacks for Callbacks<TPayload> {
    // TODO: Having the initialization of the strategy here
    // means that ProcessingStrategy and ProcessingStrategyFactory
    // have to be Send and Sync, which is really limiting and unnecessary.
    // Revisit this so that it is not the callback that perform the
    // initialization.  But we just provide a signal back to the
    // processor to do that.
    fn on_assign(&self, _: HashMap<Partition, u64>) {
        let mut stg = self.strategies.lock().unwrap();
        stg.strategy = Some(stg.processing_factory.create());
    }
    fn on_revoke(&self, _: Vec<Partition>) {
        let metrics = get_metrics();
        let start = Instant::now();

        let mut stg = self.strategies.lock().unwrap();
        match stg.strategy.as_mut() {
            None => {}
            Some(s) => {
                s.close();
                if let Ok(Some(commit_request)) = s.join(None) {
                    let mut consumer = self.consumer.lock().unwrap();
                    consumer.stage_offsets(commit_request.positions).unwrap();
                    consumer.commit_offsets().unwrap();
                }
            }
        }
        stg.strategy = None;

        metrics.timing(
            "arroyo.consumer.join.time",
            start.elapsed().as_millis() as u64,
            None,
        );

        // TODO: Figure out how to flush the metrics buffer from the recovation callback.
    }
}

impl<TPayload> Callbacks<TPayload> {
    pub fn new(
        strategies: Arc<Mutex<Strategies<TPayload>>>,
        consumer: Arc<Mutex<dyn Consumer<TPayload>>>,
    ) -> Self {
        Self {
            strategies,
            consumer,
        }
    }
}

/// A stream processor manages the relationship between a ``Consumer``
/// instance and a ``ProcessingStrategy``, ensuring that processing
/// strategies are instantiated on partition assignment and closed on
/// partition revocation.
pub struct StreamProcessor<TPayload> {
    consumer: Arc<Mutex<dyn Consumer<TPayload>>>,
    strategies: Arc<Mutex<Strategies<TPayload>>>,
    message: Option<Message<TPayload>>,
    processor_handle: ProcessorHandle,
    backpressure_timestamp: Option<Instant>,
    is_paused: bool,
    metrics_buffer: metrics_buffer::MetricsBuffer,
}

impl<TPayload: 'static> StreamProcessor<TPayload> {
    pub fn new(
        consumer: Arc<Mutex<dyn Consumer<TPayload>>>,
        processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    ) -> Self {
        let strategies = Arc::new(Mutex::new(Strategies {
            processing_factory,
            strategy: None,
        }));

        Self {
            consumer,
            strategies,
            message: None,
            processor_handle: ProcessorHandle {
                shutdown_requested: Arc::new(AtomicBool::new(false)),
            },
            backpressure_timestamp: None,
            is_paused: false,
            metrics_buffer: metrics_buffer::MetricsBuffer::new(),
        }
    }

    pub fn subscribe(&mut self, topic: Topic) {
        let callbacks: Box<dyn AssignmentCallbacks> = Box::new(Callbacks::new(
            self.strategies.clone(),
            self.consumer.clone(),
        ));
        self.consumer
            .lock()
            .unwrap()
            .subscribe(&[topic], callbacks)
            .unwrap();
    }

    pub fn run_once(&mut self) -> Result<(), RunError> {
        if self.is_paused {
            // If the consumer waas paused, it should not be returning any messages
            // on ``poll``.
            let res = self
                .consumer
                .lock()
                .unwrap()
                .poll(Some(Duration::ZERO))
                .unwrap();

            match res {
                None => {}
                Some(_) => return Err(RunError::InvalidState),
            }
        } else if self.message.is_none() {
            // Otherwise, we need to try fetch a new message from the consumer,
            // even if there is no active assignment and/or processing strategy.
            let poll_start = Instant::now();
            //TODO: Support errors properly
            match self
                .consumer
                .lock()
                .unwrap()
                .poll(Some(Duration::from_secs(1)))
            {
                Ok(msg) => {
                    self.message = msg.map(|inner| Message {
                        inner_message: InnerMessage::BrokerMessage(inner),
                    });
                    self.metrics_buffer
                        .incr_timing("arroyo.consumer.poll.time", poll_start.elapsed());
                }
                Err(error) => {
                    tracing::error!(%error, "poll error");
                    return Err(RunError::PollError);
                }
            }
        }

        let mut trait_callbacks = self.strategies.lock().unwrap();
        let Some(strategy) = trait_callbacks.strategy.as_mut() else {
            match self.message.as_ref() {
                None => return Ok(()),
                Some(_) => return Err(RunError::InvalidState),
            }
        };
        let commit_request = strategy.poll();
        match commit_request {
            Ok(None) => {}
            Ok(Some(request)) => {
                self.consumer
                    .lock()
                    .unwrap()
                    .stage_offsets(request.positions)
                    .unwrap();
                self.consumer.lock().unwrap().commit_offsets().unwrap();
            }
            Err(e) => {
                println!("TODOO: Handle invalid message {:?}", e);
            }
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
                if self.is_paused {
                    let partitions = self
                        .consumer
                        .lock()
                        .unwrap()
                        .tell()
                        .unwrap()
                        .keys()
                        .cloned()
                        .collect();

                    let res = self.consumer.lock().unwrap().resume(partitions);
                    match res {
                        Ok(()) => {
                            self.is_paused = false;
                        }
                        Err(_) => return Err(RunError::PauseError),
                    }
                }

                // Clear backpressure timestamp if it is set
                if self.backpressure_timestamp.is_some() {
                    self.metrics_buffer.incr_timing(
                        "arroyo.consumer.backpressure.time",
                        self.backpressure_timestamp.unwrap().elapsed(),
                    );
                    self.backpressure_timestamp = None;
                }
            }
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                // Put back the carried over message
                self.message = Some(message);

                if self.backpressure_timestamp.is_none() {
                    self.backpressure_timestamp = Some(Instant::now());
                }

                // If we are in the backpressure state for more than 1 second,
                // we pause the consumer and hold the message until it is
                // accepted, at which point we can resume consuming.
                if !self.is_paused && self.backpressure_timestamp.is_some() {
                    let backpressure_duration = self.backpressure_timestamp.unwrap().elapsed();

                    if backpressure_duration < Duration::from_secs(1) {
                        return Ok(());
                    }

                    tracing::warn!(
                        "Consumer is in backpressure state for more than 1 second, pausing",
                    );

                    let partitions = self
                        .consumer
                        .lock()
                        .unwrap()
                        .tell()
                        .unwrap()
                        .keys()
                        .cloned()
                        .collect();

                    let res = self.consumer.lock().unwrap().pause(partitions);
                    match res {
                        Ok(()) => {
                            self.is_paused = true;
                        }
                        Err(_) => return Err(RunError::PauseError),
                    }
                }
            }
            Err(SubmitError::InvalidMessage(message)) => {
                // TODO: Put this into the DLQ once we have one
                tracing::error!(?message, "Invalid message");
            }
        }
        Ok(())
    }

    /// The main run loop, see class docstring for more information.
    pub fn run(&mut self) -> Result<(), RunError> {
        while !self
            .processor_handle
            .shutdown_requested
            .load(Ordering::Relaxed)
        {
            if let Err(e) = self.run_once() {
                let mut trait_callbacks = self.strategies.lock().unwrap();

                if let Some(strategy) = trait_callbacks.strategy.as_mut() {
                    strategy.terminate();
                }

                drop(trait_callbacks); // unlock mutex so we can close consumer
                self.consumer.lock().unwrap().close();
                return Err(e);
            }
        }
        self.shutdown();
        Ok(())
    }

    pub fn get_handle(&self) -> ProcessorHandle {
        self.processor_handle.clone()
    }

    pub fn shutdown(&mut self) {
        self.consumer.lock().unwrap().close();
    }

    pub fn tell(self) -> HashMap<Partition, u64> {
        self.consumer.lock().unwrap().tell().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::strategies::{
        CommitRequest, InvalidMessage, ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
    };
    use super::StreamProcessor;
    use crate::backends::local::broker::LocalBroker;
    use crate::backends::local::LocalConsumer;
    use crate::backends::storages::memory::MemoryMessageStorage;
    use crate::types::{Message, Partition, Topic};
    use crate::utils::clock::SystemClock;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use uuid::Uuid;

    struct TestStrategy {
        message: Option<Message<String>>,
    }
    impl ProcessingStrategy<String> for TestStrategy {
        #[allow(clippy::manual_map)]
        fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
            Ok(match self.message.as_ref() {
                None => None,
                Some(message) => Some(CommitRequest {
                    positions: HashMap::from_iter(message.committable()),
                }),
            })
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
        let consumer = Arc::new(Mutex::new(LocalConsumer::new(
            Uuid::nil(),
            broker,
            "test_group".to_string(),
            false,
        )));

        let mut processor = StreamProcessor::new(consumer, Box::new(TestFactory {}));
        processor.subscribe(Topic::new("test1"));
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

        let consumer = Arc::new(Mutex::new(LocalConsumer::new(
            Uuid::nil(),
            broker,
            "test_group".to_string(),
            false,
        )));

        let mut processor = StreamProcessor::new(consumer, Box::new(TestFactory {}));
        processor.subscribe(Topic::new("test1"));
        let res = processor.run_once();
        assert!(res.is_ok());
        let res = processor.run_once();
        assert!(res.is_ok());

        let expected = HashMap::from([(partition, 2)]);

        assert_eq!(processor.tell(), expected)
    }
}
