use crate::types::{CogsData, CommitLogOffsets};

/// Post-write metadata — carries commit log offsets and COGS data
/// without the row payload. Lives on `PipelineBatch` during processing,
/// then extracted by `ClickHouseWriterStage` after a successful write
/// for downstream handlers.
#[derive(Clone, Debug, Default)]
pub struct BatchMetadata {
    pub commit_log_offsets: CommitLogOffsets,
    pub cogs_data: CogsData,
}
