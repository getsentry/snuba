pub mod strategies;

use crate::backends::{AssignmentCallbacks, Consumer};
use crate::types::{InnerMessage, Message, Partition, Topic};
use std::collections::HashMap;
use std::mem::replace;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

struct Strategies<TPayload: Clone> {
    processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    strategy: Option<Box<dyn ProcessingStrategy<TPayload>>>,
}

struct Callbacks<TPayload: Clone> {
    strategies: Arc<Mutex<Strategies<TPayload>>>,
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

impl<TPayload: 'static + Clone> AssignmentCallbacks for Callbacks<TPayload> {
    // TODO: Having the initialization of the strategy here
    // means that ProcessingStrategy and ProcessingStrategyFactory
    // have to be Send and Sync, which is really limiting and unnecessary.
    // Revisit this so that it is not the callback that perform the
    // initialization.  But we just provide a signal back to the
    // processor to do that.
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {
        let mut stg = self.strategies.lock().unwrap();
        stg.strategy = Some(stg.processing_factory.create());
    }
    fn on_revoke(&mut self, _: Vec<Partition>) {
        let mut stg = self.strategies.lock().unwrap();
        match stg.strategy.as_mut() {
            None => {}
            Some(s) => {
                s.close();
                s.join(None);
            }
        }
        stg.strategy = None;
    }
}

impl<TPayload: Clone> Callbacks<TPayload> {
    pub fn new(strategies: Arc<Mutex<Strategies<TPayload>>>) -> Self {
        Self { strategies }
    }
}

/// A stream processor manages the relationship between a ``Consumer``
/// instance and a ``ProcessingStrategy``, ensuring that processing
/// strategies are instantiated on partition assignment and closed on
/// partition revocation.
pub struct StreamProcessor<'a, TPayload: Clone> {
    consumer: Box<dyn Consumer<'a, TPayload> + 'a>,
    strategies: Arc<Mutex<Strategies<TPayload>>>,
    message: Option<Message<TPayload>>,
    processor_handle: ProcessorHandle,
}

impl<'a, TPayload: 'static + Clone> StreamProcessor<'a, TPayload> {
    pub fn new(
        consumer: Box<dyn Consumer<'a, TPayload> + 'a>,
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
        }
    }

    pub fn subscribe(&mut self, topic: Topic) {
        let callbacks: Box<dyn AssignmentCallbacks> =
            Box::new(Callbacks::new(self.strategies.clone()));
        self.consumer.subscribe(&[topic], callbacks).unwrap();
    }

    pub fn run_once(&mut self) -> Result<(), RunError> {
        let message_carried_over = self.message.is_some();

        if message_carried_over {
            // If a message was carried over from the previous run, the consumer
            // should be paused and not returning any messages on ``poll``.
            let res = self.consumer.poll(Some(Duration::ZERO)).unwrap();
            match res {
                None => {}
                Some(_) => return Err(RunError::InvalidState),
            }
        } else {
            // Otherwise, we need to try fetch a new message from the consumer,
            // even if there is no active assignment and/or processing strategy.
            let msg = self.consumer.poll(Some(Duration::ZERO));
            //TODO: Support errors properly
            match msg {
                Ok(None) => {
                    self.message = None;
                }
                Ok(Some(inner)) => {
                    self.message = Some(Message {
                        inner_message: InnerMessage::BrokerMessage(inner),
                    });
                }
                Err(e) => {
                    log::error!("poll error: {}", e);
                    return Err(RunError::PollError);
                }
            }
        }

        let mut trait_callbacks = self.strategies.lock().unwrap();
        match trait_callbacks.strategy.as_mut() {
            None => match self.message.as_ref() {
                None => {}
                Some(_) => return Err(RunError::InvalidState),
            },
            Some(strategy) => {
                let commit_request = strategy.poll();
                match commit_request {
                    None => {}
                    Some(request) => {
                        self.consumer.stage_offsets(request.positions).unwrap();
                        self.consumer.commit_offsets().unwrap();
                    }
                };

                let msg = replace(&mut self.message, None);
                if let Some(msg_s) = msg {
                    let ret = strategy.submit(msg_s);
                    match ret {
                        Ok(()) => {}
                        Err(_) => {
                            // If the processing strategy rejected our message, we need
                            // to pause the consumer and hold the message until it is
                            // accepted, at which point we can resume consuming.
                            let partitions =
                                self.consumer.tell().unwrap().keys().cloned().collect();
                            if message_carried_over {
                                let res = self.consumer.pause(partitions);
                                match res {
                                    Ok(()) => {}
                                    Err(_) => return Err(RunError::PauseError),
                                }
                            } else {
                                let res = self.consumer.resume(partitions);
                                match res {
                                    Ok(()) => {}
                                    Err(_) => return Err(RunError::PauseError),
                                }
                            }
                        }
                    }
                }
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
            let ret = self.run_once();
            match ret {
                Ok(()) => {}
                Err(e) => {
                    let mut trait_callbacks = self.strategies.lock().unwrap();

                    match trait_callbacks.strategy.as_mut() {
                        None => {}
                        Some(strategy) => {
                            strategy.terminate();
                        }
                    }
                    drop(trait_callbacks); // unlock mutex so we can close consumer
                    self.consumer.close();
                    return Err(e);
                }
            }
        }
        self.shutdown();
        Ok(())
    }

    pub fn get_handle(&self) -> ProcessorHandle {
        self.processor_handle.clone()
    }

    pub fn shutdown(&mut self) {
        self.consumer.close();
    }

    pub fn tell(self) -> HashMap<Partition, u64> {
        self.consumer.tell().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::strategies::{
        CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
    };
    use super::StreamProcessor;
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
        fn poll(&mut self) -> Option<CommitRequest> {
            match self.message.as_ref() {
                None => None,
                Some(message) => Some(CommitRequest {
                    positions: HashMap::from_iter(message.committable().into_iter()),
                }),
            }
        }

        fn submit(&mut self, message: Message<String>) -> Result<(), MessageRejected> {
            self.message = Some(message);
            Ok(())
        }

        fn close(&mut self) {}

        fn terminate(&mut self) {}

        fn join(&mut self, _: Option<Duration>) -> Option<CommitRequest> {
            None
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

        let topic1 = Topic {
            name: "test1".to_string(),
        };

        let _ = broker.create_topic(topic1, 1);
        broker
    }

    #[test]
    fn test_processor() {
        let mut broker = build_broker();
        let consumer = Box::new(LocalConsumer::new(
            Uuid::nil(),
            &mut broker,
            "test_group".to_string(),
            false,
        ));

        let mut processor = StreamProcessor::new(consumer, Box::new(TestFactory {}));
        processor.subscribe(Topic {
            name: "test1".to_string(),
        });
        let res = processor.run_once();
        assert!(res.is_ok())
    }

    #[test]
    fn test_consume() {
        let mut broker = build_broker();
        let topic1 = Topic {
            name: "test1".to_string(),
        };
        let partition = Partition {
            topic: topic1,
            index: 0,
        };
        let _ = broker.produce(&partition, "message1".to_string());
        let _ = broker.produce(&partition, "message2".to_string());

        let consumer = Box::new(LocalConsumer::new(
            Uuid::nil(),
            &mut broker,
            "test_group".to_string(),
            false,
        ));

        let mut processor = StreamProcessor::new(consumer, Box::new(TestFactory {}));
        processor.subscribe(Topic {
            name: "test1".to_string(),
        });
        let res = processor.run_once();
        assert!(res.is_ok());
        let res = processor.run_once();
        assert!(res.is_ok());

        let expected = HashMap::from([(partition, 2)]);

        assert_eq!(processor.tell(), expected)
    }
}
