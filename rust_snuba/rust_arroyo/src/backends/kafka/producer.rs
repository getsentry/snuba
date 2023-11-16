use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer as ArroyoProducer;
use crate::backends::ProducerError;
use crate::types::TopicOrPartition;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};

pub struct KafkaProducer {
    producer: ThreadedProducer<DefaultProducerContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Self {
        let config_obj: ClientConfig = config.into();
        let threaded_producer: ThreadedProducer<_> = config_obj.create().unwrap();

        Self {
            producer: threaded_producer,
        }
    }
}

impl ArroyoProducer<KafkaPayload> for KafkaProducer {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        let topic = match destination {
            TopicOrPartition::Topic(topic) => topic.as_str(),
            TopicOrPartition::Partition(partition) => partition.topic.as_str(),
        };

        let msg_key = &*payload.key.unwrap_or_default();
        let msg_payload = &*(payload.payload.unwrap_or_default());

        let mut base_record = BaseRecord::to(topic).payload(msg_payload).key(msg_key);

        let partition = match destination {
            TopicOrPartition::Topic(_) => None,
            TopicOrPartition::Partition(partition) => Some(partition.index),
        };

        if let Some(index) = partition {
            base_record = base_record.partition(index as i32)
        }

        self.producer
            .send(base_record)
            .map_err(|_| ProducerError::ProducerErrorred)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaProducer;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::Producer;
    use crate::types::{Topic, TopicOrPartition};
    use std::sync::Arc;
    #[test]
    fn test_producer() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

        let producer = KafkaProducer::new(configuration);

        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some(Arc::new("asdf".as_bytes().to_vec())),
        };
        producer
            .produce(&destination, payload)
            .expect("Message produced")
    }
}
