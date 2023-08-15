use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer as ArroyoProducer;
use crate::types::TopicOrPartition;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;

pub struct KafkaProducer {
    producer: Option<BaseProducer>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Self {
        let config_obj: ClientConfig = config.into();
        let base_producer: BaseProducer<_> = config_obj.create().unwrap();

        Self {
            producer: Some(base_producer),
        }
    }
}

impl KafkaProducer {
    pub fn poll(&self) {
        let producer = self.producer.as_ref().unwrap();
        producer.poll(Duration::ZERO);
    }

    pub fn flush(&self) {
        let producer = self.producer.as_ref().unwrap();
        producer.flush(Duration::from_millis(5000));
    }
}

impl ArroyoProducer<KafkaPayload> for KafkaProducer {
    fn produce(&self, destination: &TopicOrPartition, payload: &KafkaPayload) {
        let topic = match destination {
            TopicOrPartition::Topic(topic) => topic.name.as_ref(),
            TopicOrPartition::Partition(partition) => partition.topic.name.as_ref(),
        };

        // TODO: Fix the KafkaPayload type to avoid all this cloning
        let payload_copy = payload.clone();
        let msg_key = payload_copy.key.unwrap_or_default();
        let msg_payload = payload_copy.payload.unwrap_or_default();

        let mut base_record = BaseRecord::to(topic).payload(&msg_payload).key(&msg_key);

        let partition = match destination {
            TopicOrPartition::Topic(_) => None,
            TopicOrPartition::Partition(partition) => Some(partition.index),
        };

        if let Some(index) = partition {
            base_record = base_record.partition(index as i32)
        }

        let producer = self.producer.as_ref().expect("Not closed");

        producer.send(base_record).expect("Something went wrong");
    }
    fn close(&mut self) {
        self.producer = None;
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaProducer;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::Producer;
    use crate::types::{Topic, TopicOrPartition};
    #[test]
    fn test_producer() {
        let topic = Topic {
            name: "test".to_string(),
        };
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

        let mut producer = KafkaProducer::new(configuration);

        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some("asdf".as_bytes().to_vec()),
        };
        producer.produce(&destination, &payload);
        producer.close();
    }
}
