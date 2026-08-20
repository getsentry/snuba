use std::time::Instant;

use chrono::Utc;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::{counter, gauge, timer};

use crate::pull::batch::batch_metadata::BatchMetadata;
use crate::pull::batch::pipeline_batch::PipelineBatch;
use crate::pull::writer::ClickHouseWriter;

/// Stage that writes a PipelineBatch to ClickHouse.
///
/// Consumes the row data and constructs `BatchMetadata` with
/// commit log offsets, COGS data, and write stats for downstream
/// handlers. Row bytes are freed after the write.
pub struct ClickHouseWriterStage {
    writer: Box<dyn ClickHouseWriter>,
}

impl ClickHouseWriterStage {
    pub fn new(writer: impl ClickHouseWriter + 'static) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }
}

impl Stage for ClickHouseWriterStage {
    type In = PipelineBatch;
    type Out = BatchMetadata;

    async fn process(
        &self,
        envelope: PipelineEnvelope<PipelineBatch>,
    ) -> StageResult<BatchMetadata> {
        let total_rows = envelope.payload.rows.num_rows;
        let num_bytes = envelope.payload.rows.encoded_rows.len();
        let earliest_kafka_ts = envelope.payload.earliest_kafka_ts;
        let body = envelope.payload.rows.encoded_rows;
        let commit_log_offsets = envelope.payload.commit_log_offsets;
        let cogs_data = envelope.payload.cogs_data;

        if num_bytes == 0 {
            tracing::debug!("skipping write of empty payload ({} rows)", total_rows);
            let metadata = BatchMetadata {
                commit_log_offsets,
                cogs_data,
            };
            return StageResult::Emit(PipelineEnvelope::new(
                metadata,
                envelope.metadata,
                envelope.raw,
            ));
        }

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
                if let Ok(latency) = (Utc::now() - earliest_kafka_ts).to_std() {
                    timer!("insertions.latency_ms", latency);
                }

                tracing::info!(
                    rows = total_rows,
                    bytes = num_bytes,
                    "wrote batch to ClickHouse"
                );

                let metadata = BatchMetadata {
                    commit_log_offsets,
                    cogs_data,
                };
                StageResult::Emit(PipelineEnvelope::new(
                    metadata,
                    envelope.metadata,
                    envelope.raw,
                ))
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
