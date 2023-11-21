use std::cmp::max;

use chrono::{DateTime, Utc};
use rust_arroyo::utils::metrics::{BoxMetrics, Metrics};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct BytesInsertBatch {
    rows: BytesRows,
    sum_message_timestamp_secs: f64,
    max_message_timestamp_secs: i64,
}

impl BytesInsertBatch {
    pub fn new(timestamp: DateTime<Utc>, rows: BytesRows) -> Self {
        let unix_timestamp = timestamp.timestamp();
        BytesInsertBatch {
            rows,
            sum_message_timestamp_secs: unix_timestamp as f64,
            max_message_timestamp_secs: unix_timestamp,
        }
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.rows.encoded_rows.extend(other.rows.encoded_rows);
        self.rows.num_rows += other.rows.num_rows;
        self.sum_message_timestamp_secs += other.sum_message_timestamp_secs;
        self.max_message_timestamp_secs = max(
            self.max_message_timestamp_secs,
            other.max_message_timestamp_secs,
        );
        self
    }

    pub fn record_message_latency(&self, metrics: &BoxMetrics) {
        let write_time = Utc::now();

        let into_latency = |ts: DateTime<Utc>| (write_time - ts).num_seconds().try_into().ok();

        if let Some(ts) =
            DateTime::from_timestamp(self.max_message_timestamp_secs, 0).and_then(into_latency)
        {
            metrics.timing("insertions.max_latency_ms", ts, None);
        } else {
            tracing::error!("overflow while trying to calculate insertions.max_latency_ms metric");
        }

        if let Some(latency) = DateTime::from_timestamp(
            (self.sum_message_timestamp_secs / self.rows.num_rows as f64) as i64,
            0,
        )
        .and_then(into_latency)
        {
            metrics.timing("insertions.latency_ms", latency, None);
        } else {
            tracing::error!("overflow while trying to calculate insertions.latency_ms metric");
        }
    }

    pub fn len(&self) -> usize {
        self.rows.num_rows
    }

    pub fn get_encoded_rows(&self) -> &[u8] {
        &self.rows.encoded_rows
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct BytesRows {
    encoded_rows: Vec<u8>,
    num_rows: usize,
}

impl BytesRows {
    pub fn from_rows(rows: impl IntoIterator<Item = Vec<u8>>) -> Self {
        let mut encoded_rows = Vec::new();
        let mut num_rows = 0;
        for row in rows {
            encoded_rows.extend(row);
            encoded_rows.extend(b"\n");
            num_rows += 1;
        }

        BytesRows {
            num_rows,
            encoded_rows,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KafkaMessageMetadata {
    pub partition: u16,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}
