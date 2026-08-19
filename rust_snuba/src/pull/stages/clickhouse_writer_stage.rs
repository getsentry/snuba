use std::time::Instant;

use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::{counter, gauge, timer};

use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::writer::ClickHouseWriter;

/// Stage that writes a PipelineBatch to ClickHouse.
///
/// Receives a merged `PipelineBatch` from the batch stage and sends
/// its encoded rows via the injected writer.
///
/// The writer is stored as `Box<dyn ClickHouseWriter>` — no generics.
/// Use `ClickhouseClient` for production, `DryRunWriter` for staging,
/// `MockWriter` for tests.
pub struct ClickHouseWriterStage {
    writer: Box<dyn ClickHouseWriter>,
    skip_write: bool,
}

impl ClickHouseWriterStage {
    pub fn new(writer: impl ClickHouseWriter + 'static) -> Self {
        Self {
            writer: Box::new(writer),
            skip_write: false,
        }
    }

    pub fn with_skip_write(writer: impl ClickHouseWriter + 'static, skip_write: bool) -> Self {
        Self {
            writer: Box::new(writer),
            skip_write,
        }
    }
}

impl Stage for ClickHouseWriterStage {
    type In = PipelineBatch;
    type Out = PipelineBatch;

    async fn process(
        &self,
        envelope: PipelineEnvelope<PipelineBatch>,
    ) -> StageResult<PipelineBatch> {
        let total_rows = envelope.payload.rows.num_rows;
        let num_bytes = envelope.payload.rows.encoded_rows.len();

        if num_bytes == 0 {
            tracing::debug!("skipping write of empty payload ({} rows)", total_rows);
            return StageResult::Emit(envelope);
        }

        if self.skip_write {
            tracing::info!("skipping write of {} rows (skip_write=true)", total_rows);
            return StageResult::Emit(envelope);
        }

        let write_start = Instant::now();

        let body = envelope.payload.rows.encoded_rows.clone();
        let result = self.writer.write(body).await;

        timer!(
            "insertions.batch_write_ms",
            write_start.elapsed(),
            "success" => result.is_ok()
        );

        match result {
            Ok(()) => {
                counter!("insertions.batch_write_bytes", num_bytes as i64);
                counter!("insertions.batch_write_msgs", total_rows as i64);
                gauge!("insertions.batch_flush_bytes", num_bytes as i64);
                gauge!("insertions.batch_flush_msgs", total_rows as i64);

                tracing::info!(
                    rows = total_rows,
                    bytes = num_bytes,
                    "wrote batch to ClickHouse"
                );
                StageResult::Emit(envelope)
            }
            Err(e) => {
                tracing::error!("ClickHouse write failed: {}", e);
                StageResult::Fail(e.into())
            }
        }
    }

    fn name(&self) -> &str {
        "clickhouse_writer"
    }
}
