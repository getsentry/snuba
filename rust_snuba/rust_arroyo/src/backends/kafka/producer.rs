use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer as ArroyoProducer;
use crate::backends::ProducerError;
use crate::types::TopicOrPartition;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};

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
        let base_record = payload.to_base_record(destination);

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
    #[test]
    fn test_producer() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new(vec!["127.0.0.1:9092".to_string()], Default::default());

        let producer = KafkaProducer::new(configuration);

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        producer
            .produce(&destination, payload)
            .expect("Message produced")
    }
}
