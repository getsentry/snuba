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
    use super::*;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::processing::strategies::InvalidMessage;
    use crate::types::{BrokerMessage, InnerMessage, Partition, Topic};
    use chrono::Utc;
    use std::sync::Arc;

    #[test]
    fn test_produce() {
        let config = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my_group".to_string(),
            "latest".to_string(),
            false,
            None,
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
                payload: KafkaPayload {
                    key: None,
                    headers: None,
                    payload: Some(Arc::new(payload_str.clone())),
                },
                partition,
                offset: 0,
                timestamp: Utc::now(),
            }),
        };

        strategy.submit(message).unwrap();
        strategy.close();
        let _ = strategy.join(None);
    }
}
