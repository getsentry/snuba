use sentry_arroyo::processing::stream::Buffer;

use crate::types::InsertBatch;

/// Buffer for InsertBatch items. Reports byte size from the encoded row data.
pub struct InsertBatchBuffer {
    items: Vec<InsertBatch>,
}

impl InsertBatchBuffer {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }
}

impl Buffer<InsertBatch> for InsertBatchBuffer {
    fn push(&mut self, item: InsertBatch) -> u64 {
        let bytes = item.rows.encoded_rows.len() as u64;
        self.items.push(item);
        bytes
    }

    fn len(&self) -> u64 {
        self.items.len() as u64
    }

    fn flush(&mut self) -> Vec<InsertBatch> {
        std::mem::take(&mut self.items)
    }
}
