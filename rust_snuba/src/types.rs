use std::cmp::min;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::timer;
use sentry_protos::snuba::v1::TraceItemType;
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

/// Returns the friendly name for a TraceItemType, e.g. "span" instead of "TRACE_ITEM_TYPE_SPAN".
fn item_type_name(item_type: TraceItemType) -> String {
    item_type
        .as_str_name()
        .strip_prefix("TRACE_ITEM_TYPE_")
        .unwrap_or(item_type.as_str_name())
        .to_lowercase()
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ItemTypeMetrics {
    pub counts: BTreeMap<TraceItemType, u64>,
    pub bytes_processed: BTreeMap<TraceItemType, usize>,
}

impl ItemTypeMetrics {
    pub fn new() -> Self {
        Self {
            counts: BTreeMap::new(),
            bytes_processed: BTreeMap::new(),
        }
    }

    pub fn record_item(&mut self, item_type: TraceItemType, size_bytes: usize) {
        self.counts
            .entry(item_type)
            .and_modify(|count| *count += 1)
            .or_insert(1);

        self.bytes_processed
            .entry(item_type)
            .and_modify(|bytes| *bytes += size_bytes)
            .or_insert(size_bytes);
    }

    pub fn merge(&mut self, other: ItemTypeMetrics) {
        for (item_type, count) in other.counts {
            self.counts
                .entry(item_type)
                .and_modify(|curr| *curr += count)
                .or_insert(count);
        }

        for (item_type, size) in other.bytes_processed {
            self.bytes_processed
                .entry(item_type)
                .and_modify(|curr| *curr += size)
                .or_insert(size);
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
    pub item_type_metrics: Option<ItemTypeMetrics>,
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
            item_type_metrics: None,
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
    pub rows: R,

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

    item_type_metrics: ItemTypeMetrics,
}

impl<R> BytesInsertBatch<R> {
    /// Create a new BytesInsertBatch with all fields specified.
    /// For most use cases, prefer `from_rows()` which uses sensible defaults.
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
            item_type_metrics: Default::default(),
        }
    }

    /// Create a BytesInsertBatch with just rows, using defaults for all other fields.
    /// Use builder methods to set optional fields as needed.
    ///
    /// # Example
    /// ```text
    /// let batch = BytesInsertBatch::from_rows(rows)
    ///     .with_message_timestamp(timestamp)
    ///     .with_cogs_data(cogs_data);
    /// ```
    pub fn from_rows(rows: R) -> Self {
        Self {
            rows,
            message_timestamp: Default::default(),
            origin_timestamp: Default::default(),
            sentry_received_timestamp: Default::default(),
            commit_log_offsets: Default::default(),
            cogs_data: Default::default(),
            item_type_metrics: Default::default(),
        }
    }

    /// Set the message timestamp (when the message was inserted into the Kafka topic)
    pub fn with_message_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.message_timestamp = LatencyRecorder::from(timestamp);
        self
    }

    /// Set the origin timestamp (when the event was received by Relay)
    pub fn with_origin_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.origin_timestamp = LatencyRecorder::from(timestamp);
        self
    }

    /// Set the sentry received timestamp (when received by ingest consumer in Sentry)
    pub fn with_sentry_received_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.sentry_received_timestamp = LatencyRecorder::from(timestamp);
        self
    }

    /// Set the commit log offsets
    pub fn with_commit_log_offsets(mut self, offsets: CommitLogOffsets) -> Self {
        self.commit_log_offsets = offsets;
        self
    }

    /// Set the COGS data
    pub fn with_cogs_data(mut self, cogs_data: CogsData) -> Self {
        self.cogs_data = cogs_data;
        self
    }

    /// Set the item type metrics
    pub fn with_item_type_metrics(mut self, item_type_metrics: ItemTypeMetrics) -> Self {
        self.item_type_metrics = item_type_metrics;
        self
    }

    pub fn commit_log_offsets(&self) -> &CommitLogOffsets {
        &self.commit_log_offsets
    }

    pub fn cogs_data(&self) -> &CogsData {
        &self.cogs_data
    }

    pub fn clone_meta(&self) -> BytesInsertBatch<()> {
        BytesInsertBatch {
            rows: (),
            message_timestamp: self.message_timestamp.clone(),
            origin_timestamp: self.origin_timestamp.clone(),
            sentry_received_timestamp: self.sentry_received_timestamp.clone(),
            commit_log_offsets: self.commit_log_offsets.clone(),
            cogs_data: self.cogs_data.clone(),
            item_type_metrics: self.item_type_metrics.clone(),
        }
    }

    pub fn take(self) -> (R, BytesInsertBatch<()>) {
        let new = BytesInsertBatch {
            rows: (),
            message_timestamp: self.message_timestamp,
            origin_timestamp: self.origin_timestamp,
            sentry_received_timestamp: self.sentry_received_timestamp,
            commit_log_offsets: self.commit_log_offsets,
            cogs_data: self.cogs_data,
            item_type_metrics: self.item_type_metrics,
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

    pub fn emit_item_type_metrics(&self) {
        use sentry_arroyo::counter;

        for (item_type, count) in &self.item_type_metrics.counts {
            counter!(
                "insertions.item_type_count",
                *count,
                "item_type" => item_type_name(*item_type)
            );
        }

        for (item_type, size) in &self.item_type_metrics.bytes_processed {
            counter!(
                "insertions.item_bytes_processed",
                *size as u64,
                "item_type" => item_type_name(*item_type)
            );
        }
    }
}

impl BytesInsertBatch<RowData> {
    pub fn len(&self) -> usize {
        self.rows.num_rows
    }

    pub fn merge(mut self, other: BytesInsertBatch<RowData>) -> Self {
        self.rows.encoded_rows.extend(other.rows.encoded_rows);
        self.rows.num_rows += other.rows.num_rows;
        self.commit_log_offsets.merge(other.commit_log_offsets);
        self.message_timestamp.merge(other.message_timestamp);
        self.origin_timestamp.merge(other.origin_timestamp);
        self.sentry_received_timestamp
            .merge(other.sentry_received_timestamp);
        self.cogs_data.merge(other.cogs_data);
        self.item_type_metrics.merge(other.item_type_metrics);
        self
    }
}

impl<T> BytesInsertBatch<Vec<T>> {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.rows.extend(other.rows);
        self.commit_log_offsets.merge(other.commit_log_offsets);
        self.message_timestamp.merge(other.message_timestamp);
        self.origin_timestamp.merge(other.origin_timestamp);
        self.sentry_received_timestamp
            .merge(other.sentry_received_timestamp);
        self.cogs_data.merge(other.cogs_data);
        self.item_type_metrics.merge(other.item_type_metrics);
        self
    }
}

/// The return value of message processors that produce typed rows for RowBinary insertion.
#[derive(Clone, Debug)]
pub struct TypedInsertBatch<T> {
    pub rows: Vec<T>,
    pub origin_timestamp: Option<DateTime<Utc>>,
    pub sentry_received_timestamp: Option<DateTime<Utc>>,
    pub cogs_data: Option<CogsData>,
    pub item_type_metrics: Option<ItemTypeMetrics>,
}

impl<T> TypedInsertBatch<T> {
    pub fn from_rows(rows: Vec<T>, origin_timestamp: Option<DateTime<Utc>>) -> Self {
        Self {
            rows,
            origin_timestamp,
            sentry_received_timestamp: None,
            cogs_data: None,
            item_type_metrics: None,
        }
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

    #[test]
    fn test_item_type_metrics_merge() {
        let mut metrics1 = ItemTypeMetrics::new();
        metrics1.record_item(TraceItemType::Span, 100);
        metrics1.record_item(TraceItemType::Span, 150);
        metrics1.record_item(TraceItemType::Log, 200);

        let mut metrics2 = ItemTypeMetrics::new();
        metrics2.record_item(TraceItemType::Span, 50);
        metrics2.record_item(TraceItemType::Log, 75);

        metrics1.merge(metrics2);

        // Verify counts are merged correctly
        assert_eq!(metrics1.counts.get(&TraceItemType::Span), Some(&3));
        assert_eq!(metrics1.counts.get(&TraceItemType::Log), Some(&2));

        // Verify bytes_processed are merged correctly
        assert_eq!(
            metrics1.bytes_processed.get(&TraceItemType::Span),
            Some(&300)
        ); // 100 + 150 + 50
        assert_eq!(
            metrics1.bytes_processed.get(&TraceItemType::Log),
            Some(&275)
        ); // 200 + 75
    }
}
