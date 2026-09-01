use futures::future::BoxFuture;

use crate::strategies::clickhouse::writer_v2::ClickhouseClient;

use super::clickhouse_writer::ClickHouseWriter;

/// Live writer that delegates to the existing `ClickhouseClient`.
/// Compression, retries, and HTTP transport are all handled by the client.
impl ClickHouseWriter for ClickhouseClient {
    fn write(&self, body: Vec<u8>) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.send(body).await?;
            Ok(())
        })
    }
}
