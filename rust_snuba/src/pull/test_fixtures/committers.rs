use std::collections::HashMap;
use std::sync::Mutex;

use sentry_arroyo::processing::stream::OffsetCommitter;
use sentry_arroyo::types::Partition;

/// Mock committer that records committed offsets for test assertions.
pub struct MockCommitter {
    committed: Mutex<Vec<HashMap<Partition, u64>>>,
}

impl MockCommitter {
    pub fn new() -> Self {
        Self {
            committed: Mutex::new(Vec::new()),
        }
    }

    pub fn committed(&self) -> Vec<HashMap<Partition, u64>> {
        self.committed.lock().unwrap().clone()
    }
}

impl OffsetCommitter for MockCommitter {
    fn commit_offsets(
        &self,
        positions: &HashMap<Partition, u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.committed.lock().unwrap().push(positions.clone());
        Ok(())
    }
}
