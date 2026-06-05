use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Response};
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use sentry_arroyo::{counter, timer};

use crate::config::ClickhouseConfig;
use crate::runtime_config::{get_load_balancing_config, get_max_insert_block_size};
use crate::types::{BytesInsertBatch, RowData};

fn clickhouse_task_runner(
    client: Arc<ClickhouseClient>,
    skip_write: bool,
) -> impl TaskRunner<BytesInsertBatch<RowData>, BytesInsertBatch<()>, anyhow::Error> {
    move |message: Message<BytesInsertBatch<RowData>>| -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        let skip_write = skip_write;
        let client = client.clone();

        Box::pin(async move {
            let (empty_message, insert_batch) = message.take();
            let batch_len = insert_batch.len();
            let (rows, empty_batch) = insert_batch.take();
            let encoded_rows = rows.into_encoded_rows();
            let num_bytes = encoded_rows.len();

            let write_start = SystemTime::now();

            // we can receive empty batches since we configure Reduce to flush empty batches, in
            // order to still be able to commit. in that case we want to skip the I/O to clickhouse
            // though.
            if encoded_rows.is_empty() {
                tracing::debug!(
                    "skipping write of empty payload ({} rows)",
                    batch_len
                );
            } else if skip_write {
                tracing::info!("skipping write of {} rows", batch_len);
            } else {
                tracing::debug!("performing write");

                let response = client
                    .send(encoded_rows, RetryConfig::default())
                    .await
                    .map_err(RunTaskError::Other)?;

                tracing::debug!(?response);
                tracing::info!("Inserted {} rows", batch_len);
                let write_finish = SystemTime::now();
                if let Ok(elapsed) = write_finish.duration_since(write_start) {
                    timer!("insertions.batch_write_ms", elapsed);
                }
            }


            counter!("insertions.batch_write_bytes", num_bytes as i64);
            counter!("insertions.batch_write_msgs", batch_len as i64);
            empty_batch.record_message_latency();
            empty_batch.emit_item_type_metrics();

            Ok(empty_message.replace(empty_batch))
        })
    }
}

pub struct ClickhouseWriterStep<N> {
    inner: RunTaskInThreads<BytesInsertBatch<RowData>, BytesInsertBatch<()>, anyhow::Error, N>,
}

/// Wire format the writer step posts to ClickHouse with. Picking the right
/// value here is the only thing the consumer needs to know — the pipeline
/// shape, batching, and retry behavior are identical across formats.
#[derive(Clone, Copy, Debug)]
pub enum InsertFormat {
    JsonEachRow,
    RowBinary,
}

impl InsertFormat {
    fn as_str(self) -> &'static str {
        match self {
            InsertFormat::JsonEachRow => "JSONEachRow",
            InsertFormat::RowBinary => "RowBinary",
        }
    }
}

impl<N> ClickhouseWriterStep<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    /// `columns`: if `Some`, expand into the SQL as
    /// `INSERT INTO {table} (col1, col2, ...) FORMAT ...`. Required for
    /// `RowBinary`, which would otherwise fall back to the table's positional
    /// column order — a footgun whenever wire order and table order diverge.
    /// For `JSONEachRow`, pass `None` to preserve historical behavior.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
        storage_name: String,
        format: InsertFormat,
        columns: Option<&'static [&'static str]>,
    ) -> Self {
        let inner = RunTaskInThreads::new(
            next_step,
            clickhouse_task_runner(
                Arc::new(ClickhouseClient::new(
                    &cluster_config.clone(),
                    &table,
                    storage_name,
                    format,
                    columns,
                )),
                skip_write,
            ),
            concurrency,
            Some("clickhouse"),
        );

        ClickhouseWriterStep { inner }
    }
}

impl<N> ProcessingStrategy<BytesInsertBatch<RowData>> for ClickhouseWriterStep<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<RowData>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<RowData>>> {
        self.inner.submit(message)
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}

pub struct RetryConfig {
    initial_backoff_ms: f64,
    max_retries: usize,
    jitter_factor: f64, // between 0 and 1
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 500.0,
            max_retries: 4,
            jitter_factor: 0.2,
        }
    }
}

#[derive(Clone)]
pub struct ClickhouseClient {
    client: Client,
    headers: HeaderMap<HeaderValue>,
    base_url: String,
    storage_name: String,
    query: String,
}

impl ClickhouseClient {
    pub fn new(
        config: &ClickhouseConfig,
        table: &str,
        storage_name: String,
        format: InsertFormat,
        columns: Option<&[&str]>,
    ) -> ClickhouseClient {
        let mut headers = HeaderMap::with_capacity(6);
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        headers.insert(
            "X-Clickhouse-User",
            HeaderValue::from_str(&config.user).unwrap(),
        );
        headers.insert(
            "X-ClickHouse-Key",
            HeaderValue::from_str(&config.password).unwrap(),
        );
        headers.insert(
            "X-ClickHouse-Database",
            HeaderValue::from_str(&config.database).unwrap(),
        );

        let scheme = if config.secure { "https" } else { "http" };
        let host = &config.host;
        let port = &config.http_port;

        // `decompress=1` tells ClickHouse the POST body is in its native
        // compressed format (LZ4 blocks framed with CityHash128 checksums) —
        // the same wire format `clickhouse-rs` used and what
        // `clickhouse-compressor` produces. Distinct from HTTP-standard
        // `Content-Encoding: lz4`, which would need `enable_http_compression=1`.
        let mut base_url =
            format!("{scheme}://{host}:{port}?insert_distributed_sync=1&decompress=1");
        if matches!(format, InsertFormat::RowBinary) {
            // RowBinary cannot represent JSON values natively; tell ClickHouse
            // to treat any binary string targeting a JSON column as JSON text.
            base_url.push_str("&input_format_binary_read_json_as_string=1");
        }
        let columns_clause = match columns {
            Some(cols) => format!(" ({})", cols.join(", ")),
            None => String::new(),
        };
        let query = format!(
            "INSERT INTO {table}{columns_clause} FORMAT {fmt}",
            fmt = format.as_str(),
        );

        ClickhouseClient {
            client: Client::new(),
            headers,
            base_url,
            storage_name,
            query,
        }
    }

    fn build_url(&self) -> String {
        let lb_config = get_load_balancing_config(&self.storage_name);
        let mut url = format!(
            "{}&load_balancing={}",
            self.base_url, lb_config.load_balancing
        );
        if let Some(offset) = lb_config.first_offset {
            url.push_str(&format!("&load_balancing_first_offset={offset}"));
        }
        if let Some(block_size) = get_max_insert_block_size(&self.storage_name) {
            url.push_str(&format!("&max_insert_block_size={block_size}"));
        }
        url
    }

    pub async fn send(&self, body: Vec<u8>, retry_config: RetryConfig) -> anyhow::Result<Response> {
        // Compress once before the retry loop — the encoded body is identical
        // across attempts, so paying the LZ4 cost per attempt would be wasted
        // work. `bytes::Bytes` makes the per-attempt clone cheap (refcount bump).
        let body_bytes = bytes::Bytes::from(lz4_compress(&body));
        // Free the uncompressed buffer now; it would otherwise stay alive
        // across every retry attempt and backoff sleep, holding ~3× the
        // compressed size per in-flight send for the full send window.
        drop(body);

        for attempt in 0..=retry_config.max_retries {
            let url = self.build_url();
            let res = self
                .client
                .post(&url)
                .headers(self.headers.clone())
                .query(&[("query", &self.query)])
                .body(reqwest::Body::from(body_bytes.clone()))
                .send()
                .await;

            match res {
                Ok(response) => {
                    if response.status() == reqwest::StatusCode::OK {
                        return Ok(response);
                    } else {
                        let status = response.status().to_string();
                        let error_text = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "unknown error".to_string());

                        if attempt == retry_config.max_retries {
                            counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "false");
                            anyhow::bail!(
                                "error writing to clickhouse after {} attempts: {}",
                                retry_config.max_retries + 1,
                                error_text
                            );
                        }

                        counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "true");
                        tracing::warn!(
                            "ClickHouse write failed (attempt {}/{}): status={}, error={}",
                            attempt + 1,
                            retry_config.max_retries + 1,
                            status,
                            error_text
                        );
                    }
                }
                Err(e) => {
                    if attempt == retry_config.max_retries {
                        counter!("rust_consumer.clickhouse_insert_error", 1, "status" => "network_error", "retried" => "false");
                        anyhow::bail!(
                            "error writing to clickhouse after {} attempts: {}",
                            retry_config.max_retries + 1,
                            e
                        );
                    }
                    counter!("rust_consumer.clickhouse_insert_error", 1, "status" => "network_error", "retried" => "true");

                    tracing::warn!(
                        "ClickHouse write failed (attempt {}/{}): {}",
                        attempt + 1,
                        retry_config.max_retries + 1,
                        e
                    );
                }
            }

            // Calculate exponential backoff delay
            if attempt < retry_config.max_retries {
                let backoff_ms =
                    retry_config.initial_backoff_ms * (2_u64.pow(attempt as u32) as f64);
                // add/subtract up to 10% jitter (by default) to avoid every consumer retrying at the same time
                // causing too many simultaneous queries
                let jitter = rand::random::<f64>() * retry_config.jitter_factor
                    - retry_config.jitter_factor / 2.0; // Random value between (-jitter_factor/2, jitter_factor/2)
                let delay = Duration::from_millis((backoff_ms * (1.0 + jitter)).round() as u64);
                tracing::debug!(
                    "Retrying in {:?} (attempt {}/{})",
                    delay,
                    attempt + 1,
                    retry_config.max_retries
                );
                tokio::time::sleep(delay).await;
            }
        }

        unreachable!("Loop should always return or bail before reaching here");
    }
}

/// ClickHouse native compressed-block size cap. Matches the server's
/// `max_compress_block_size` default; sending larger blocks risks tripping
/// server-side decompress limits.
const LZ4_BLOCK_SIZE: usize = 1024 * 1024;

/// ClickHouse compression method identifier for LZ4 in the native block header.
const LZ4_METHOD_BYTE: u8 = 0x82;

/// CityHash128 over `data` in the wire layout ClickHouse's
/// `CompressedReadBuffer` reads: 8 little-endian bytes of the low 64-bit half
/// first, then 8 little-endian bytes of the high half.
///
/// `cityhash-rs` returns a `u128` with the halves swapped relative to that
/// convention (the canonical "low" half ends up in the upper 64 bits of the
/// returned `u128`), so a naive `to_le_bytes()` puts the wrong half first
/// and ClickHouse rejects the body with `CANNOT_DECOMPRESS / Checksum
/// doesn't match`. Rotating by 64 swaps the halves back into the order CH
/// expects.
///
/// We use CityHash 1.0.2 — that's the variant ClickHouse bundles for
/// compression checksums; the 110 variant is reserved for newer hash columns
/// and is NOT interchangeable here.
fn ch_compression_checksum(data: &[u8]) -> [u8; 16] {
    cityhash_rs::cityhash_102_128(data)
        .rotate_left(64)
        .to_le_bytes()
}

/// Encode `input` in ClickHouse's native compressed format — the same wire
/// shape `clickhouse-rs` and `clickhouse-compressor` produce, and what the
/// server expects when `decompress=1` is set in the URL.
///
/// The body is a concatenation of one or more blocks. Each block is laid out:
///
///   [0..16]  CityHash128(header || compressed), low half LE then high half LE
///   [16]     LZ4_METHOD_BYTE (0x82)
///   [17..21] u32 LE: compressed size INCLUDING the 9-byte header
///   [21..25] u32 LE: uncompressed size of this block
///   [25..]   raw LZ4 block bytes (no frame, no prepended size)
///
/// The 9-byte (method + sizes) header is hashed together with the compressed
/// bytes so the checksum guards both.
fn lz4_compress(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() / 2 + 32);
    for chunk in input.chunks(LZ4_BLOCK_SIZE) {
        let compressed = lz4_flex::block::compress(chunk);
        let compressed_with_header = 9u32 + compressed.len() as u32;
        let uncompressed_size = chunk.len() as u32;

        let block_start = out.len();
        out.extend_from_slice(&[0u8; 16]);
        out.push(LZ4_METHOD_BYTE);
        out.extend_from_slice(&compressed_with_header.to_le_bytes());
        out.extend_from_slice(&uncompressed_size.to_le_bytes());
        out.extend_from_slice(&compressed);

        let checksum = ch_compression_checksum(&out[block_start + 16..]);
        out[block_start..block_start + 16].copy_from_slice(&checksum);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    fn make_test_config() -> ClickhouseConfig {
        ClickhouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or("127.0.0.1".to_string()),
            port: std::env::var("CLICKHOUSE_PORT")
                .unwrap_or("9000".to_string())
                .parse::<u16>()
                .unwrap(),
            secure: std::env::var("CLICKHOUSE_SECURE")
                .unwrap_or("false".to_string())
                .to_lowercase()
                == "true",
            http_port: std::env::var("CLICKHOUSE_HTTP_PORT")
                .unwrap_or("8123".to_string())
                .parse::<u16>()
                .unwrap(),
            user: std::env::var("CLICKHOUSE_USER").unwrap_or("default".to_string()),
            password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or("".to_string()),
            database: std::env::var("CLICKHOUSE_DATABASE").unwrap_or("default".to_string()),
        }
    }

    #[tokio::test]
    async fn it_works() -> Result<(), reqwest::Error> {
        crate::testutils::initialize_python();
        let config = make_test_config();
        println!("config: {config:?}");
        let client = ClickhouseClient::new(
            &config,
            "querylog_local",
            "test_storage".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        let url = client.build_url();
        assert!(url.contains("load_balancing=in_order"));
        assert!(url.contains("insert_distributed_sync"));
        assert!(url.contains("decompress=1"));
        println!("running test");
        let res = client.send(b"[]".to_vec(), RetryConfig::default()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }

    #[test]
    fn test_url_with_runtime_config_override() {
        crate::testutils::initialize_python();
        let config = make_test_config();
        let client = ClickhouseClient::new(
            &config,
            "test_table",
            "writer_v2_lb_test".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        // Default: in_order
        let url = client.build_url();
        assert!(url.contains("load_balancing=in_order"));
        assert!(!url.contains("load_balancing_first_offset"));

        // Override to first_or_random with offset
        crate::runtime_config::patch_str_config_for_test(
            "clickhouse_load_balancing:writer_v2_lb_test",
            Some("first_or_random"),
        );
        crate::runtime_config::patch_str_config_for_test(
            "clickhouse_load_balancing_first_offset:writer_v2_lb_test",
            Some("1"),
        );

        let url = client.build_url();
        assert!(url.contains("load_balancing=first_or_random"));
        assert!(url.contains("load_balancing_first_offset=1"));
    }

    #[test]
    fn test_url_with_max_insert_block_size() {
        crate::testutils::initialize_python();
        let config = make_test_config();
        let client = ClickhouseClient::new(
            &config,
            "test_table",
            "writer_v2_block_size_test".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        // Default (key absent): no suffix.
        let url = client.build_url();
        assert!(!url.contains("max_insert_block_size"));

        // Per-storage override at or above the ClickHouse default sets the suffix.
        crate::runtime_config::patch_str_config_for_test(
            "clickhouse_max_insert_block_size:writer_v2_block_size_test",
            Some("2000000"),
        );
        let url = client.build_url();
        assert!(url.contains("&max_insert_block_size=2000000"));

        // A different storage isn't affected.
        let other_client = ClickhouseClient::new(
            &config,
            "test_table",
            "writer_v2_other_storage".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );
        let url = other_client.build_url();
        assert!(!url.contains("max_insert_block_size"));

        // Values below the ClickHouse default (1_048_449) are rejected.
        crate::runtime_config::patch_str_config_for_test(
            "clickhouse_max_insert_block_size:writer_v2_block_size_test",
            Some("1000000"),
        );
        let url = client.build_url();
        assert!(!url.contains("max_insert_block_size"));

        // Exactly the default is accepted.
        crate::runtime_config::patch_str_config_for_test(
            "clickhouse_max_insert_block_size:writer_v2_block_size_test",
            Some("1048449"),
        );
        let url = client.build_url();
        assert!(url.contains("&max_insert_block_size=1048449"));
    }

    /// Walks a buffer of concatenated ClickHouse-native compressed blocks,
    /// verifies each block's header layout and CityHash128 checksum, and
    /// returns the concatenated decompressed payload. Used by the roundtrip
    /// tests below — kept as a helper so single-block and multi-block paths
    /// share the same decoder.
    fn decode_native_blocks(buf: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            assert!(buf.len() - pos >= 25, "truncated block header");
            let stored_checksum: [u8; 16] = buf[pos..pos + 16].try_into().unwrap();
            assert_eq!(buf[pos + 16], LZ4_METHOD_BYTE, "wrong compression method");
            let compressed_with_header =
                u32::from_le_bytes(buf[pos + 17..pos + 21].try_into().unwrap()) as usize;
            let uncompressed_size =
                u32::from_le_bytes(buf[pos + 21..pos + 25].try_into().unwrap()) as usize;
            let block_end = pos + 16 + compressed_with_header;
            assert!(block_end <= buf.len(), "block size overruns buffer");

            let computed = ch_compression_checksum(&buf[pos + 16..block_end]);
            assert_eq!(computed, stored_checksum, "checksum mismatch");

            let chunk = lz4_flex::block::decompress(&buf[pos + 25..block_end], uncompressed_size)
                .expect("decompress");
            assert_eq!(chunk.len(), uncompressed_size);
            decoded.extend_from_slice(&chunk);
            pos = block_end;
        }
        decoded
    }

    /// Guards the cityhash-rs ↔ ClickHouse byte-order convention: the wire
    /// puts the canonical "low" 64 bits first (LE), and `cityhash-rs` stores
    /// that half in the upper 64 bits of its returned `u128`. Without the
    /// rotate, this test fails AND CH would reject the body with
    /// "Checksum doesn't match" — which is exactly how this bug first
    /// surfaced (see the it_works integration test).
    #[test]
    fn test_compression_checksum_matches_clickhouse_wire_order() {
        let data = b"snuba clickhouse native compressed block payload";
        let bytes = ch_compression_checksum(data);

        let wire_low = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let wire_high = u64::from_le_bytes(bytes[8..].try_into().unwrap());

        let raw = cityhash_rs::cityhash_102_128(data);
        // cityhash-rs convention: canonical "low" in upper bits, "high" in lower.
        let canonical_low = (raw >> 64) as u64;
        let canonical_high = raw as u64;

        assert_eq!(wire_low, canonical_low);
        assert_eq!(wire_high, canonical_high);
    }

    #[test]
    fn test_lz4_compress_roundtrip_single_block() {
        let mut input = b"INSERT INTO eap_items FORMAT RowBinary\n".to_vec();
        for i in 0..1024 {
            input.push((i % 251) as u8);
        }
        assert!(input.len() < LZ4_BLOCK_SIZE);

        let compressed = lz4_compress(&input);
        assert_eq!(decode_native_blocks(&compressed), input);
    }

    #[test]
    fn test_lz4_compress_chunks_at_block_size() {
        // 2.5 blocks: exercises the chunking loop (3 blocks expected, last partial).
        let input: Vec<u8> = (0..(LZ4_BLOCK_SIZE * 2 + LZ4_BLOCK_SIZE / 2))
            .map(|i| (i % 251) as u8)
            .collect();

        let compressed = lz4_compress(&input);
        let decoded = decode_native_blocks(&compressed);
        assert_eq!(decoded.len(), input.len());
        assert_eq!(decoded, input);
    }

    #[tokio::test]
    async fn test_retry_with_exponential_backoff() {
        crate::testutils::initialize_python();
        // Test that retry logic works by using a non-existent server
        // This will trigger network errors that should be retried
        let config = ClickhouseConfig {
            host: "127.0.0.1".to_string(),
            port: 9000,
            secure: false,
            http_port: 9999, // Use a port that's not listening
            user: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
        };

        let client = ClickhouseClient::new(
            &config,
            "test_table",
            "test_storage".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        let start_time = Instant::now();
        let result = client
            .send(
                b"test data".to_vec(),
                RetryConfig {
                    initial_backoff_ms: 100.0,
                    max_retries: 4,
                    jitter_factor: 0.1,
                },
            )
            .await;
        let elapsed = start_time.elapsed();

        // Should fail after all retries
        assert!(result.is_err());

        // Should have taken at least the sum of our backoff delays
        // 90ms + 180ms + 360ms + 720ms = 1350ms minimum
        assert!(elapsed >= Duration::from_millis(1350));

        // Error message should mention the number of attempts
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("after 5 attempts"));
    }
}
