use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::backends::ConsumerError;

impl From<KafkaError> for ConsumerError {
    fn from(err: KafkaError) -> Self {
        match err {
            KafkaError::OffsetFetch(RDKafkaErrorCode::OffsetOutOfRange) => {
                ConsumerError::OffsetOutOfRange {
                    source: Box::new(err),
                }
            }
            other => ConsumerError::BrokerError(Box::new(other)),
        }
    }
}
