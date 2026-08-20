use sentry_arroyo::processing::stream::Buffer;

use super::pipeline_batch::PipelineBatch;

/// Buffer that merges PipelineBatch items into a single batch.
/// Concatenates encoded rows and merges commit log offsets + COGS data.
pub struct PipelineBatchBuffer {
    batch: Option<PipelineBatch>,
}

impl PipelineBatchBuffer {
    pub fn new() -> Self {
        Self { batch: None }
    }
}

impl Buffer<PipelineBatch> for PipelineBatchBuffer {
    type Output = PipelineBatch;

    fn push(&mut self, item: PipelineBatch) -> u64 {
        let bytes = item.rows.encoded_rows.len() as u64;
        match &mut self.batch {
            Some(existing) => existing.merge(item),
            None => self.batch = Some(item),
        }
        bytes
    }

    fn len(&self) -> u64 {
        self.batch
            .as_ref()
            .map(|b| b.rows.num_rows as u64)
            .unwrap_or(0)
    }

    fn flush(&mut self) -> PipelineBatch {
        self.batch.take().unwrap_or_else(PipelineBatch::empty)
    }
}
