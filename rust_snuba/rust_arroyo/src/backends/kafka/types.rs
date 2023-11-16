use rdkafka::message::OwnedHeaders;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct KafkaPayload {
    pub key: Option<Arc<Vec<u8>>>,
    pub headers: Option<Arc<OwnedHeaders>>,
    pub payload: Option<Arc<Vec<u8>>>,
}
