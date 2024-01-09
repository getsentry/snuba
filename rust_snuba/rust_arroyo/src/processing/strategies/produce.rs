use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use crate::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use crate::types::{Message, TopicOrPartition};
use std::sync::Arc;
use std::time::Duration;

struct ProduceMessage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: TopicOrPartition,
}

impl ProduceMessage {
    pub fn new(producer: impl Producer<KafkaPayload> + 'static, topic: TopicOrPartition) -> Self {
        ProduceMessage {
            producer: Arc::new(producer),
            topic,
        }
    }
}

impl TaskRunner<KafkaPayload, KafkaPayload> for ProduceMessage {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<KafkaPayload> {
        let producer = self.producer.clone();
        let topic = self.topic;

        Box::pin(async move {
            producer
                .produce(&topic, message.payload().clone())
                .expect("Message was produced");
            Ok(message)
        })
    }
}

pub struct Produce {
    inner: RunTaskInThreads<KafkaPayload, KafkaPayload>,
}

impl Produce {
    pub fn new<N>(
        next_step: N,
        producer: impl Producer<KafkaPayload> + 'static,
        concurrency: &ConcurrencyConfig,
        topic: TopicOrPartition,
    ) -> Self
    where
        N: ProcessingStrategy<KafkaPayload> + 'static,
    {
        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(ProduceMessage::new(producer, topic)),
            concurrency,
            Some("produce"),
        );

        Produce { inner }
    }
}

impl ProcessingStrategy<KafkaPayload> for Produce {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), SubmitError<KafkaPayload>> {
        self.inner.submit(message)
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;
    use std::time::SystemTime;

    use super::*;
    use crate::backends::kafka::config::{KafkaConfig, KafkaConsumerConfig};
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::InitialOffset;
    use crate::backends::local::broker::LocalBroker;
    use crate::backends::local::LocalProducer;
    use crate::backends::storages::memory::MemoryMessageStorage;
    use crate::processing::strategies::InvalidMessage;
    use crate::types::{BrokerMessage, InnerMessage, Partition, Topic};
    use crate::utils::clock::TestingClock;
    use chrono::Utc;

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

    impl ProcessingStrategy<KafkaPayload> for Mock {
        fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
            self.0.lock().polled = true;
            Ok(None)
        }
        fn submit(
            &mut self,
            _message: Message<KafkaPayload>,
        ) -> Result<(), SubmitError<KafkaPayload>> {
            self.0.lock().submit += 1;
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
    fn test_produce() {
        let config = KafkaConfig::new_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            Default::default(),
        );

        let partition = Partition::new(Topic::new("test"), 0);

        struct Noop {}
        impl ProcessingStrategy<KafkaPayload> for Noop {
            fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
                Ok(None)
            }
            fn submit(
                &mut self,
                _message: Message<KafkaPayload>,
            ) -> Result<(), SubmitError<KafkaPayload>> {
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

        let producer: KafkaProducer = KafkaProducer::new(config);
        let concurrency = ConcurrencyConfig::new(10);
        let mut strategy = Produce::new(
            Noop {},
            producer,
            &concurrency,
            TopicOrPartition::Topic(partition.topic),
        );

        let payload_str = "hello world".to_string().as_bytes().to_vec();
        let message = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                payload: KafkaPayload::new(None, None, Some(payload_str.clone())),
                partition,
                offset: 0,
                timestamp: Utc::now(),
            }),
        };

        strategy.submit(message).unwrap();
        strategy.close();
        let _ = strategy.join(None);
    }

    #[test]
    fn test_produce_local() {
        let orig_topic = Topic::new("orig-topic");
        let result_topic = Topic::new("result-topic");
        let clock = TestingClock::new(SystemTime::now());
        let storage = MemoryMessageStorage::default();
        let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));
        broker.create_topic(result_topic, 1).unwrap();

        let broker = Arc::new(Mutex::new(broker));
        let producer = LocalProducer::new(broker.clone());

        let next_step = Mock::new();
        let counts = next_step.counts();
        let concurrency_config = ConcurrencyConfig::new(1);
        let mut strategy = Produce::new(
            next_step,
            producer,
            &concurrency_config,
            result_topic.into(),
        );

        let value = br#"{"something": "something"}"#.to_vec();
        let data = KafkaPayload::new(None, None, Some(value.clone()));
        let now = chrono::Utc::now();

        let message = Message::new_broker_message(data, Partition::new(orig_topic, 0), 1, now);
        strategy.submit(message.clone()).unwrap();
        strategy.join(None).unwrap();

        let produced_message = broker
            .lock()
            .storage_mut()
            .consume(&Partition::new(result_topic, 0), 0)
            .unwrap()
            .unwrap();

        assert_eq!(produced_message.payload.payload().unwrap(), &value);

        assert!(broker
            .lock()
            .storage_mut()
            .consume(&Partition::new(result_topic, 0), 1)
            .unwrap()
            .is_none());

        strategy.poll().unwrap();
        assert_eq!(
            *counts.lock(),
            Counts {
                submit: 1,
                polled: true
            }
        );

        strategy.submit(message.clone()).unwrap();
        strategy.join(None).unwrap();
        assert_eq!(
            *counts.lock(),
            Counts {
                submit: 2,
                polled: true,
            }
        );

        let mut result = Ok(());
        for _ in 0..3 {
            result = strategy.submit(message.clone());
            if result.is_err() {
                break;
            }
        }

        assert!(result.is_err());
    }
}
