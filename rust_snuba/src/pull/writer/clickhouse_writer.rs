use std::future::Future;
use std::pin::Pin;

/// Trait for writing encoded rows to ClickHouse (or a test double).
///
/// Object-safe — can be used as `Box<dyn ClickHouseWriter>`.
/// The `write` method is called once per batch flush, so the
/// `Pin<Box<dyn Future>>` allocation is negligible.
///
/// The real `ClickhouseClient` implements this — it compresses, retries,
/// and POSTs to ClickHouse over HTTP. `DryRunWriter` implements it for
/// staging — it compresses (real CPU cost), sleeps (simulated
/// network latency), and drops the data.
pub trait ClickHouseWriter: Send + Sync {
    fn write(&self, body: Vec<u8>)
        -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}

impl<T: ClickHouseWriter> ClickHouseWriter for std::sync::Arc<T> {
    fn write(
        &self,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        (**self).write(body)
    }
}
