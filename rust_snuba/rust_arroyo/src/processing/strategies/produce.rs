
use futures::{Future, task};
use futures::executor::block_on;
use futures::stream::FuturesOrdered;
use log::warn;
use crate::backends::Producer;
use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy,
};
use crate::types::{Message, TopicOrPartition};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use futures::executor::ThreadPool;

pub struct ProduceFuture {
    // pub producer: &'a KafkaProducer,
    // destination: &'a TopicOrPartition,
    pub producer: Arc<KafkaProducer>,
    destination: Arc<TopicOrPartition>,
    payload: KafkaPayload,
    pub completed: bool,
}

// type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
type Task = Pin<Box<ProduceFuture>>;

impl Future for ProduceFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>, ) -> std::task::Poll<()> {
        self.producer.produce(&self.destination, &self.payload);
        return std::task::Poll::Ready(())
    }
}
pub struct Produce<TPayload: Clone + Send + Sync> {
    pub producer: Arc<KafkaProducer>,
    pub next_step: Box<dyn ProcessingStrategy<TPayload>>,
    queue: VecDeque<(Message<TPayload>, Task, JoinHandle<()>)>,
    topic: Arc<TopicOrPartition>,
    closed: bool,
    max_queue_size: usize,
    pool: ThreadPool
}

impl Produce<KafkaPayload> {
    pub fn new(producer: KafkaProducer, next_step: Box<dyn ProcessingStrategy<KafkaPayload>>, topic:TopicOrPartition) -> Self {
        Produce {
            producer: Arc::new(producer),
            next_step,
            queue: VecDeque::new(),
            topic: Arc::new(topic),
            closed: false,
            max_queue_size: 1000,
            pool: ThreadPool::new().unwrap()
        }
    }
}

impl ProcessingStrategy<KafkaPayload>
    for Produce< KafkaPayload>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        while !self.queue.is_empty(){
            let (message, mut produce_fut) = self.queue.pop_front().unwrap();

            let waker = task::noop_waker();
            let mut cx = Context::from_waker(&waker);

            match produce_fut.as_mut().poll(&mut cx) {
                task::Poll::Ready(val) => {
                    // future is done,
                    produce_fut.completed = true;
                },
                task::Poll::Pending => todo!(),
            } ;
            // match Future::poll(produce_fut, &mut cx) {
            //     std::task::Poll::Ready(val) => {
            //         // future is done,
            //         produce_fut.completed = true;

            //     }
            //     std::task::Poll::Pending => {
            //         break;
            //     }
            // };
            self.next_step.poll();
            self.next_step.submit(message).unwrap()
        }
        return None
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        if self.closed {
            panic!("Attempted to submit a message to a closed Produce strategy")
        }
        if self.queue.len() >= self.max_queue_size {
            return Err(MessageRejected)
        }

        let produce_fut = ProduceFuture {
            producer: Arc::clone(&self.producer),
            destination: Arc::clone(&self.topic),
            payload: message.payload.clone(),
            completed: false,
        };
        // spawn the future
        let handle = tokio::spawn(produce_fut);

        self.queue.push_back((message, Box::pin(produce_fut), handle));
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
            let (message, produce_fut) = self.queue.pop_front().unwrap();
            self.next_step.poll();
            self.next_step.submit(message).unwrap();

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
    use std::sync::Arc;
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
            producer: Arc::new(producer),
            queue: VecDeque::new(),
            topic: Arc::new(TopicOrPartition::Topic(partition.topic.clone())),
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
