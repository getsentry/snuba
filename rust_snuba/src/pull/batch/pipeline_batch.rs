use chrono::{DateTime, Utc};

use crate::types::{CogsData, CommitLogEntry, CommitLogOffsets, InsertBatch, RowData};

/// Batch type for the pull-based pipeline. Wraps the processor's
/// `InsertBatch` with pipeline metadata needed for commit log and COGS.
///
/// Created by `ProcessorStage` from each processed message.
/// Merged by `PipelineBatchBuffer` during batching.
#[derive(Clone)]
pub struct PipelineBatch {
    pub rows: RowData,
    pub commit_log_offsets: CommitLogOffsets,
    pub cogs_data: CogsData,
}

impl PipelineBatch {
    /// Create an empty batch (used when flushing an empty buffer).
    pub fn empty() -> Self {
        Self {
            rows: RowData::default(),
            commit_log_offsets: CommitLogOffsets::default(),
            cogs_data: CogsData::default(),
        }
    }

    /// Create from a processor's InsertBatch and the source message's
    /// partition, offset, and timestamp.
    pub fn from_insert_batch(
        batch: InsertBatch,
        partition: u16,
        offset: u64,
        timestamp: DateTime<Utc>,
    ) -> Self {
        let mut commit_log_offsets = CommitLogOffsets::default();
        commit_log_offsets.0.insert(
            partition,
            CommitLogEntry {
                // offset + 1: Kafka commit convention — the next offset to consume.
                // Matches the push model's processor (strategies/processor.rs:62).
                offset: offset + 1,
                orig_message_ts: timestamp,
                // Use origin_timestamp from the InsertBatch (sentry receive time),
                // not the Kafka message timestamp. Matches the push model
                // (strategies/processor.rs:64).
                received_p99: batch.origin_timestamp.into_iter().collect(),
            },
        );

        Self {
            rows: batch.rows,
            commit_log_offsets,
            cogs_data: batch.cogs_data.unwrap_or_default(),
        }
    }

    /// Merge another batch into this one. Concatenates rows,
    /// merges offsets and COGS data.
    pub fn merge(&mut self, other: PipelineBatch) {
        self.rows.encoded_rows.extend(other.rows.encoded_rows);
        self.rows.num_rows += other.rows.num_rows;
        self.commit_log_offsets.merge(other.commit_log_offsets);
        self.cogs_data.merge(other.cogs_data);
    }
}
