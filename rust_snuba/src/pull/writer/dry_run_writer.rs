use std::time::Duration;

use crate::strategies::clickhouse::writer_v2::lz4_compress;

use super::clickhouse_writer::ClickHouseWriter;

/// Writer that compresses data using ClickHouse's native LZ4 format
/// (real CPU cost), simulates network latency, and drops the result.
///
/// Used in staging/dry-run deployments to exercise the full pipeline
/// without writing to ClickHouse.
pub struct DryRunWriter {
    latency: Duration,
}

impl DryRunWriter {
    /// Create a dry-run writer with simulated network latency.
    pub fn new(latency: Duration) -> Self {
        Self { latency }
    }
}

impl ClickHouseWriter for DryRunWriter {
    async fn write(&self, body: Vec<u8>) -> anyhow::Result<()> {
        let raw_bytes = body.len();

        // Real CPU cost: ClickHouse native LZ4 compression with CityHash128
        // checksums — same function the production writer uses.
        let compressed = lz4_compress(&body);
        let compressed_bytes = compressed.len();
        drop(compressed);

        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }

        tracing::info!(
            raw_bytes,
            compressed_bytes,
            "dry-run: would have written {} bytes ({} compressed) to ClickHouse",
            raw_bytes,
            compressed_bytes,
        );

        Ok(())
    }
}
