use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use crate::types::{Message, TopicOrPartition};
use std::sync::Arc;
use std::time::Duration;

struct ProduceMessage {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: Arc<TopicOrPartition>,
}

impl ProduceMessage {
    pub fn new(producer: impl Producer<KafkaPayload> + 'static, topic: TopicOrPartition) -> Self {
        ProduceMessage {
            producer: Arc::new(producer),
            topic: Arc::new(topic),
        }
    }
}

impl TaskRunner<KafkaPayload, KafkaPayload> for ProduceMessage {
    fn get_task(&self, message: Message<KafkaPayload>) -> RunTaskFunc<KafkaPayload> {
        let producer = self.producer.clone();
        let topic = self.topic.clone();

        Box::pin(async move {
            producer.produce(&topic, &message.payload());
            Ok(message)
        })
    }
}

pub struct Produce {
    inner: Box<dyn ProcessingStrategy<KafkaPayload>>,
}

impl Produce {
    pub fn new<N>(
        next_step: N,
        producer: impl Producer<KafkaPayload> + 'static,
        concurrency: usize,
        topic: TopicOrPartition,
    ) -> Self
    where
        N: ProcessingStrategy<KafkaPayload> + 'static,
    {
        let inner = Box::new(RunTaskInThreads::new(
            next_step,
            Box::new(ProduceMessage::new(producer, topic)),
            concurrency,
        ));

        Produce { inner }
    }
}

impl ProcessingStrategy<KafkaPayload> for Produce {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<KafkaPayload>,
    ) -> Result<(), MessageRejected<KafkaPayload>> {
        self.inner.submit(message)
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::Produce;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
    use crate::types::{BrokerMessage, InnerMessage};
    use crate::types::{Message, Partition, Topic, TopicOrPartition};
    use chrono::Utc;
    use std::time::Duration;

    #[test]
    fn test_produce() {
        let config = KafkaConfig::new_consumer_config(
            vec![std::env::var("DEFAULT_BROKERS").unwrap_or("127.0.0.1:9092".to_string())],
            "my_group".to_string(),
            "latest".to_string(),
            false,
            None,
        );

        let partition = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 0,
        };

        struct Noop {}
        impl ProcessingStrategy<KafkaPayload> for Noop {
            fn poll(&mut self) -> Option<CommitRequest> {
                None
            }
            fn submit(
                &mut self,
                _message: Message<KafkaPayload>,
            ) -> Result<(), MessageRejected<KafkaPayload>> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
                None
            }
        }

        let producer: KafkaProducer = KafkaProducer::new(config);
        let mut strategy = Produce::new(
            Noop {},
            producer,
            10,
            TopicOrPartition::Topic(partition.topic.clone()),
        );

        let payload_str = "hello world".to_string().as_bytes().to_vec();
        let message = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                payload: KafkaPayload {
                    key: None,
                    headers: None,
                    payload: Some(payload_str.clone()),
                },
                partition: partition,
                offset: 0,
                timestamp: Utc::now(),
            }),
        };

        strategy.submit(message).unwrap();
        strategy.close();
        let _ = strategy.join(None);
    }
}
