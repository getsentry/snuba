use std::future::Future;
use std::pin::Pin;

use crate::strategies::clickhouse::writer_v2::ClickhouseClient;

use super::clickhouse_writer::ClickHouseWriter;

/// Live writer that delegates to the existing `ClickhouseClient`.
/// Compression, retries, and HTTP transport are all handled by the client.
impl ClickHouseWriter for ClickhouseClient {
    fn write(
        &self,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.send(body).await?;
            Ok(())
        })
    }
}
