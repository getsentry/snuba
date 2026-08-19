use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use crate::pull::writer::ClickHouseWriter;

/// Record of a single write call, for test assertions.
#[derive(Debug, Clone)]
pub struct WriteCall {
    pub raw_bytes: usize,
}

/// Test mock that records write calls for assertions.
pub struct MockWriter {
    calls: Mutex<Vec<WriteCall>>,
}

impl MockWriter {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> Vec<WriteCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl ClickHouseWriter for MockWriter {
    fn write(
        &self,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(WriteCall {
                raw_bytes: body.len(),
            });
            Ok(())
        })
    }
}
