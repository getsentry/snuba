use std::cmp::min;

use chrono::{DateTime, Utc};
use rust_arroyo::utils::metrics::BoxMetrics;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type CommitLogOffsets = BTreeMap<u16, (u64, DateTime<Utc>)>;

#[derive(Debug, Clone)]
struct LatencyRecorder {
    sum_timestamps: f64,
    earliest_timestamp: u64,
    num_values: usize,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        LatencyRecorder {
            sum_timestamps: 0.0,
            earliest_timestamp: u64::MAX,
            num_values: 0,
        }
    }
}

impl From<DateTime<Utc>> for LatencyRecorder {
    fn from(value: DateTime<Utc>) -> Self {
        let value = value.timestamp_millis();
        LatencyRecorder {
            sum_timestamps: value as f64,
            earliest_timestamp: value as u64,
            num_values: 1,
        }
    }
}

impl LatencyRecorder {
    fn merge(&mut self, other: Self) {
        self.sum_timestamps += other.sum_timestamps;
        self.earliest_timestamp = min(self.earliest_timestamp, other.earliest_timestamp);
        self.num_values += other.num_values;
    }

    fn max_value_ms(&self, write_time: DateTime<Utc>) -> u64 {
        let write_time = write_time.timestamp_millis() as u64;
        write_time.saturating_sub(self.earliest_timestamp)
    }

    fn avg_value_ms(&self, write_time: DateTime<Utc>) -> u64 {
        let write_time = write_time.timestamp_millis() as u64;
        (write_time as f64 - (self.sum_timestamps / self.num_values as f64)) as u64
    }

    fn send_metric(&self, metrics: &BoxMetrics, write_time: DateTime<Utc>, metric_name: &str) {
        if self.num_values == 0 {
            return;
        }

        metrics.timing(
            &format!("insertions.max_{}_ms", metric_name),
            self.max_value_ms(write_time),
            None,
        );

        metrics.timing(
            &format!("insertions.{}_ms", metric_name),
            self.avg_value_ms(write_time),
            None,
        );
    }
}

/// The return value of message processors.
///
/// NOTE: In Python, this struct crosses a serialization boundary, and so this struct is somewhat
/// sensitive to serialization speed. If there are additional things that should be returned from
/// the Rust message processor that are not necessary in Python, it's probably best to duplicate
/// this struct for Python as there it can be an internal type.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InsertBatch {
    pub rows: RowData,
    pub origin_timestamp: Option<DateTime<Utc>>,
    pub sentry_received_timestamp: Option<DateTime<Utc>>,
}

impl InsertBatch {
    pub fn from_rows<T>(rows: impl IntoIterator<Item = T>) -> anyhow::Result<Self>
    where
        T: Serialize,
    {
        let rows = RowData::from_rows(rows)?;
        Ok(Self {
            rows,
            origin_timestamp: None,
            sentry_received_timestamp: None,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct BytesInsertBatch {
    rows: RowData,

    /// when the message was inserted into the snuba topic
    ///
    /// In Python this aggregate value is not explicitly tracked on BytesInsertBatch, as the batch
    /// type contains a list of the original kafka messages which all contain the individual
    /// timestamp values in metadata.
    message_timestamp: LatencyRecorder,

    /// when the event was received by Relay
    origin_timestamp: LatencyRecorder,

    /// when it was received by the ingest consumer in Sentry
    ///
    /// May not be recorded for some datasets. This is specifically used for metrics datasets, where
    /// this represents the latency from ingest-metrics (i.e. before the metrics indexer) to
    /// insertion into clickhouse
    sentry_received_timestamp: LatencyRecorder,

    // For each partition we store the offset and timestamp to be produced to the commit log
    commit_log_offsets: CommitLogOffsets,
}

impl BytesInsertBatch {
    pub fn new(
        rows: RowData,
        message_timestamp: DateTime<Utc>,
        origin_timestamp: Option<DateTime<Utc>>,
        sentry_received_timestamp: Option<DateTime<Utc>>,
        commit_log_offsets: CommitLogOffsets,
    ) -> Self {
        BytesInsertBatch {
            rows,
            message_timestamp: message_timestamp.into(),
            origin_timestamp: origin_timestamp
                .map(LatencyRecorder::from)
                .unwrap_or_default(),
            sentry_received_timestamp: sentry_received_timestamp
                .map(LatencyRecorder::from)
                .unwrap_or_default(),
            commit_log_offsets,
        }
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.rows
            .encoded_rows
            .extend_from_slice(&other.rows.encoded_rows);
        self.commit_log_offsets.extend(other.commit_log_offsets);
        self.message_timestamp.merge(other.message_timestamp);
        self.origin_timestamp.merge(other.origin_timestamp);
        self.sentry_received_timestamp
            .merge(other.sentry_received_timestamp);
        self
    }

    pub fn record_message_latency(&self, metrics: &BoxMetrics) {
        let write_time = Utc::now();

        self.message_timestamp
            .send_metric(metrics, write_time, "latency");
        self.origin_timestamp
            .send_metric(metrics, write_time, "end_to_end_latency");
        self.sentry_received_timestamp
            .send_metric(metrics, write_time, "sentry_received_latency");
    }

    pub fn len(&self) -> usize {
        self.rows.encoded_rows.len()
    }

    pub fn encoded_rows(&self) -> &Vec<Vec<u8>> {
        &self.rows.encoded_rows
    }

    pub fn commit_log_offsets(&self) -> &CommitLogOffsets {
        &self.commit_log_offsets
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq)]
pub struct RowData {
    encoded_rows: Vec<Vec<u8>>,
}

impl RowData {
    pub fn from_rows<T>(rows: impl IntoIterator<Item = T>) -> anyhow::Result<Self>
    where
        T: Serialize,
    {
        let mut encoded_rows = Vec::new();
        for row in rows {
            let data: Vec<u8> = serde_json::to_vec(&row)?;
            encoded_rows.push(data);
        }

        Ok(RowData { encoded_rows })
    }

    pub fn from_encoded_rows(rows: Vec<Vec<u8>>) -> Self {
        RowData { encoded_rows: rows }
    }
}

#[derive(Clone, Debug)]
pub struct KafkaMessageMetadata {
    pub partition: u16,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn test_latency_recorder_basic() {
        // emulate how Reduce works when creating a batch for inserting into clickhouse, because
        // that's where we use it in practice. Reduce starts with a default() accumulator
        let mut accumulator = LatencyRecorder::default();

        // then there's one row being merged into the current batch
        let now = Utc.timestamp_opt(61, 0).unwrap();
        accumulator.merge(LatencyRecorder::from(now));

        // then another row
        let now = Utc.timestamp_opt(63, 0).unwrap();
        accumulator.merge(LatencyRecorder::from(now));

        // finally, when flushing the batch, we record the metric. let's see if LatencyRecorder did
        // the math right:
        assert_eq!(accumulator.num_values, 2);
        let now = Utc.timestamp_opt(65, 0).unwrap();
        assert_eq!(accumulator.max_value_ms(now), 4000);
        assert_eq!(accumulator.avg_value_ms(now), 3000);
    }
}
