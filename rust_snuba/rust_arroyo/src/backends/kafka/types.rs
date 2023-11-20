use rdkafka::message::OwnedHeaders;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct KafkaPayloadInner {
    pub key: Option<Vec<u8>>,
    pub headers: Option<OwnedHeaders>,
    pub payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct KafkaPayload {
    inner: Arc<KafkaPayloadInner>,
}

impl KafkaPayload {
    pub fn new(
        key: Option<Vec<u8>>,
        // TODO: OwnedHeaders is an internal rdkafka thing, try to get rid of it here
        headers: Option<OwnedHeaders>,
        payload: Option<Vec<u8>>,
    ) -> Self {
        Self {
            inner: Arc::new(KafkaPayloadInner {
                key,
                headers,
                payload,
            }),
        }
    }

    pub fn key(&self) -> Option<&Vec<u8>> {
        self.inner.key.as_ref()
    }

    pub fn headers(&self) -> Option<&OwnedHeaders> {
        self.inner.headers.as_ref()
    }

    pub fn payload(&self) -> Option<&Vec<u8>> {
        self.inner.payload.as_ref()
    }
}
