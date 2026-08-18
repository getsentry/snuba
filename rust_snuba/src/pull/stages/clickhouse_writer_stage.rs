use std::time::Instant;

use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::{counter, gauge, timer};

use crate::pull::writer::ClickHouseWriter;
use crate::types::InsertBatch;

/// Stage that writes batched InsertBatch data to ClickHouse.
///
/// Receives `Vec<InsertBatch>` from the batch stage, concatenates
/// their encoded rows, and sends them via the injected writer.
///
/// Generic over the writer — use `ClickhouseClient` for production,
/// `DryRunWriter` for testing/staging.
pub struct ClickHouseWriterStage<W: ClickHouseWriter> {
    writer: W,
    skip_write: bool,
}

impl<W: ClickHouseWriter> ClickHouseWriterStage<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            skip_write: false,
        }
    }

    pub fn with_skip_write(writer: W, skip_write: bool) -> Self {
        Self { writer, skip_write }
    }
}

impl<W: ClickHouseWriter> Stage for ClickHouseWriterStage<W> {
    type In = Vec<InsertBatch>;
    type Out = Vec<InsertBatch>;

    async fn process(
        &self,
        envelope: PipelineEnvelope<Vec<InsertBatch>>,
    ) -> StageResult<Vec<InsertBatch>> {
        // Concatenate all encoded rows from the batch.
        let mut body = Vec::new();
        let mut total_rows = 0;
        for batch in &envelope.payload {
            body.extend_from_slice(&batch.rows.encoded_rows);
            total_rows += batch.rows.num_rows;
        }

        if body.is_empty() {
            tracing::debug!("skipping write of empty payload ({} rows)", total_rows);
            return StageResult::Emit(envelope);
        }

        if self.skip_write {
            tracing::info!("skipping write of {} rows (skip_write=true)", total_rows);
            return StageResult::Emit(envelope);
        }

        let num_bytes = body.len();
        let write_start = Instant::now();

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
