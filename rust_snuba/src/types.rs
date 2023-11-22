use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowData {
    pub rows: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BytesInsertBatch {
    pub rows: Vec<Vec<u8>>,
    // For each partition we store the offset and timestamp to be produced to the commit log
    pub commit_log_offsets: BTreeMap<u16, (u64, DateTime<Utc>)>,
}

#[derive(Clone, Debug)]
pub struct KafkaMessageMetadata {
    pub partition: u16,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}
