
use log::warn;
use crate::backends::Producer;
use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy,
};
use crate::types::{Message, TopicOrPartition};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub struct Produce<TPayload: Clone + Send + Sync> {
    pub producer: KafkaProducer,
    pub next_step: Box<dyn ProcessingStrategy<TPayload>>,
    queue: VecDeque<Message<TPayload>>,
    topic: TopicOrPartition,
    closed: bool,
    max_queue_size: usize,
}

// TODO: make the queue store futures
// TODO: make this generic
impl Produce<KafkaPayload> {
    pub fn new(producer: KafkaProducer, next_step: Box<dyn ProcessingStrategy<KafkaPayload>>, topic:TopicOrPartition) -> Self {
        Produce {
            producer,
            next_step,
            queue: VecDeque::new(),
            topic,
            closed: false,
            max_queue_size: 1000,
        }
    }
}

impl ProcessingStrategy<KafkaPayload>
    for Produce<KafkaPayload>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        while !self.queue.is_empty(){
            let message = self.queue.pop_front();
            self.next_step.poll();
            self.next_step.submit(message.unwrap()).unwrap()
        }
        return None
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        if self.queue.len() >= self.max_queue_size {
            return Err(MessageRejected)
        }
        print!("Producing message: {:?}", &message.payload.payload);
        self.producer.produce(&self.topic, &message.payload);
        self.producer.flush();
        self.queue.push_back(message);
        return Ok(());
    }

    fn close(&mut self) {
        self.closed = true;
    }

    fn terminate(&mut self) {
        self.closed = true;
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        let start = Instant::now();
        let mut remaining: Option<Duration> = None;

        while !self.queue.is_empty(){
            if let Some(timeout) = timeout {
                remaining = Some(timeout - start.elapsed());
                if remaining.unwrap() <= Duration::from_secs(0) {
                    warn!("Timeout reached while waiting for the queue to be empty");
                    break;
                }
            }
            let message = self.queue.pop_front();
            self.next_step.poll();
            self.next_step.submit(message.unwrap()).unwrap();

        }

        self.next_step.close();
        self.next_step.join(remaining);
        return None;
    }
}

#[cfg(test)]
mod tests {
    use super::Produce;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::strategies::{
        CommitRequest, MessageRejected, ProcessingStrategy,
    };
    use crate::types::{Message, Partition, Topic, TopicOrPartition};
    use chrono::Utc;
    use std::collections::VecDeque;
    use std::time::Duration;

    #[test]
    fn test_produce() {
        let config = KafkaConfig::new_consumer_config(
            vec!["localhost:9092".to_string()],
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
            fn submit(&mut self, _message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
                Ok(())
            }
            fn close(&mut self) {}
            fn terminate(&mut self) {}
            fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
                None
            }
        }

        let producer: KafkaProducer = KafkaProducer::new(config);

        let mut strategy: Produce<KafkaPayload> = Produce {
            next_step: Box::new(Noop {}),
            producer: producer,
            queue: VecDeque::new(),
            topic: TopicOrPartition::Topic(partition.topic.clone()),
            closed: false,
            max_queue_size: 1000,
        };

        let payload_str = "hello world".to_string().as_bytes().to_vec();
        strategy
            .submit(Message::new(
                partition,
                0,
                KafkaPayload { key: None, headers: None, payload: Some(payload_str) },
                Utc::now(),
            ))
            .unwrap();
    }
}
