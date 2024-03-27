use std::cmp::min;
use std::collections::BTreeMap;

use crate::strategies::clickhouse::batch::HttpBatch;

use chrono::{DateTime, Utc};
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::timer;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq)]
pub struct CommitLogEntry {
    pub offset: u64,
    pub orig_message_ts: DateTime<Utc>,
    pub received_p99: Vec<DateTime<Utc>>,
}

#[derive(Default, Debug, Clone)]
pub struct CommitLogOffsets(pub BTreeMap<u16, CommitLogEntry>);

impl CommitLogOffsets {
    fn merge(&mut self, other: CommitLogOffsets) {
        for (partition, other_entry) in other.0 {
            self.0
                .entry(partition)
                .and_modify(|entry| {
                    entry.offset = other_entry.offset;
                    entry.orig_message_ts = other_entry.orig_message_ts;
                    entry.received_p99.extend(&other_entry.received_p99);
                })
                .or_insert(other_entry);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CogsData {
    pub data: BTreeMap<String, u64>, // app_feature: bytes_len
}

impl CogsData {
    fn merge(&mut self, other: CogsData) {
        for (k, v) in other.data {
            self.data
                .entry(k)
                .and_modify(|curr| *curr += v)
                .or_insert(v);
        }
    }
}

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

    fn send_metric(&self, write_time: DateTime<Utc>, metric_name: &str) {
        if self.num_values == 0 {
            return;
        }

        timer!(
            format_args!("insertions.max_{}_ms", metric_name),
            self.max_value_ms(write_time)
        );

        timer!(
            format_args!("insertions.{}_ms", metric_name),
            self.avg_value_ms(write_time),
        );
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReplacementData {
    pub key: Vec<u8>, // Project id
    // Replacement message bytes. Newline-delimited series of json payloads
    pub value: Vec<u8>,
}

impl From<ReplacementData> for KafkaPayload {
    fn from(value: ReplacementData) -> KafkaPayload {
        KafkaPayload::new(Some(value.key), None, Some(value.value))
    }
}

impl From<KafkaPayload> for ReplacementData {
    fn from(value: KafkaPayload) -> ReplacementData {
        ReplacementData {
            key: value.key().unwrap().clone(),
            value: value.payload().unwrap().clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum InsertOrReplacement<T> {
    Insert(T),
    Replacement(ReplacementData),
}

/// The return value of message processors.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InsertBatch {
    pub rows: RowData,
    pub origin_timestamp: Option<DateTime<Utc>>,
    pub sentry_received_timestamp: Option<DateTime<Utc>>,
    pub cogs_data: Option<CogsData>,
}

impl InsertBatch {
    pub fn from_rows<T>(
        rows: impl IntoIterator<Item = T>,
        origin_timestamp: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Self>
    where
        T: Serialize,
    {
        let rows = RowData::from_rows(rows)?;
        Ok(Self {
            rows,
            origin_timestamp,
            sentry_received_timestamp: None,
            cogs_data: None,
        })
    }

    /// In case the processing function wants to skip the message, we return an empty batch.
    /// But instead of having the caller send an empty batch, lets make an explicit api for
    /// skipping. This way we can change the implementation later if we want to. Skipping ensures
    /// that the message is committed but not processed.
    pub fn skip() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BytesInsertBatch<R> {
    rows: R,

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

    cogs_data: CogsData,
}

impl<R> BytesInsertBatch<R> {
    pub fn new(
        rows: R,
        message_timestamp: Option<DateTime<Utc>>,
        origin_timestamp: Option<DateTime<Utc>>,
        sentry_received_timestamp: Option<DateTime<Utc>>,
        commit_log_offsets: CommitLogOffsets,
        cogs_data: CogsData,
    ) -> Self {
        BytesInsertBatch {
            rows,
            message_timestamp: message_timestamp
                .map(LatencyRecorder::from)
                .unwrap_or_default(),
            origin_timestamp: origin_timestamp
                .map(LatencyRecorder::from)
                .unwrap_or_default(),
            sentry_received_timestamp: sentry_received_timestamp
                .map(LatencyRecorder::from)
                .unwrap_or_default(),
            commit_log_offsets,
            cogs_data,
        }
    }

    pub fn commit_log_offsets(&self) -> &CommitLogOffsets {
        &self.commit_log_offsets
    }

    pub fn cogs_data(&self) -> &CogsData {
        &self.cogs_data
    }

    pub fn take(self) -> (R, BytesInsertBatch<()>) {
        let new = BytesInsertBatch {
            rows: (),
            message_timestamp: self.message_timestamp,
            origin_timestamp: self.origin_timestamp,
            sentry_received_timestamp: self.sentry_received_timestamp,
            commit_log_offsets: self.commit_log_offsets,
            cogs_data: self.cogs_data,
        };

        (self.rows, new)
    }

    pub fn record_message_latency(&self) {
        let write_time = Utc::now();

        self.message_timestamp.send_metric(write_time, "latency");
        self.origin_timestamp
            .send_metric(write_time, "end_to_end_latency");
        self.sentry_received_timestamp
            .send_metric(write_time, "sentry_received_latency");
    }
}

impl BytesInsertBatch<RowData> {
    pub fn len(&self) -> usize {
        self.rows.num_rows
    }
}

impl BytesInsertBatch<HttpBatch> {
    pub fn merge(mut self, other: BytesInsertBatch<RowData>) -> Self {
        self.rows
            .write_rows(&other.rows)
            .expect("failed to write rows to channel");
        self.commit_log_offsets.merge(other.commit_log_offsets);
        self.message_timestamp.merge(other.message_timestamp);
        self.origin_timestamp.merge(other.origin_timestamp);
        self.sentry_received_timestamp
            .merge(other.sentry_received_timestamp);
        self.cogs_data.merge(other.cogs_data);
        self
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq)]
pub struct RowData {
    pub encoded_rows: Vec<u8>,
    pub num_rows: usize,
}

impl RowData {
    pub fn from_rows<T>(rows: impl IntoIterator<Item = T>) -> anyhow::Result<Self>
    where
        T: Serialize,
    {
        let mut encoded_rows = Vec::new();
        let mut num_rows = 0;
        for row in rows {
            serde_json::to_writer(&mut encoded_rows, &row)?;
            debug_assert!(encoded_rows.ends_with(b"}"));
            encoded_rows.push(b'\n');
            num_rows += 1;
        }

        Ok(RowData {
            num_rows,
            encoded_rows,
        })
    }

    pub fn from_encoded_rows(rows: Vec<Vec<u8>>) -> Self {
        let mut encoded_rows = Vec::new();
        let mut num_rows = 0;
        for row in rows {
            encoded_rows.extend_from_slice(&row);
            encoded_rows.push(b'\n');
            num_rows += 1;
        }

        RowData {
            encoded_rows,
            num_rows,
        }
    }

    pub fn into_encoded_rows(self) -> Vec<u8> {
        self.encoded_rows
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
