/// Trait for writing encoded rows to ClickHouse (or a test double).
///
/// The real `ClickhouseClient` implements this — it compresses, retries,
/// and POSTs to ClickHouse over HTTP. `DryRunWriter` implements it for
/// staging — it compresses (real CPU cost), sleeps (simulated
/// network latency), and drops the data.
pub trait ClickHouseWriter: Send + Sync {
    fn write(&self, body: Vec<u8>) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

impl<T: ClickHouseWriter> ClickHouseWriter for std::sync::Arc<T> {
    async fn write(&self, body: Vec<u8>) -> anyhow::Result<()> {
        (**self).write(body).await
    }
}
