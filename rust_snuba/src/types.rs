use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct BytesInsertBatch {
    pub rows: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct KafkaMessageMetadata {
    pub partition: u16,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}
