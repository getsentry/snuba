use rdkafka::message::OwnedHeaders;

#[derive(Clone, Debug)]
pub struct KafkaPayload {
    pub key: Option<Vec<u8>>,
    pub headers: Option<OwnedHeaders>,
    pub payload: Option<Vec<u8>>,
}
