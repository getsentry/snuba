use std::sync::Arc;
use std::time::{Duration, Instant};

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
use crate::options::{
    get_clickhouse_write_client_timeouts, get_clickhouse_write_retry, get_load_balancing_config,
    get_max_insert_block_size,
};
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

            let write_start = Instant::now();

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

                let result = client.send(encoded_rows).await;

                timer!(
                    "insertions.batch_write_ms",
                    write_start.elapsed(),
                    "success" => result.is_ok()
                );

                let response = result.map_err(RunTaskError::Other)?;

                tracing::debug!(?response);
                tracing::info!("Inserted {} rows", batch_len);
            }


            counter!("insertions.batch_write_bytes", num_bytes as i64);
            counter!("insertions.batch_write_msgs", batch_len as i64);
            empty_batch.record_message_latency();
            empty_batch.emit_item_type_metrics();

            Ok(empty_message.replace(empty_batch))
        })
    }
}

/// Wire format for the INSERT. Module-internal: callers pick [`JsonWriterStep`]
/// or [`RowBinaryWriterStep`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum InsertFormat {
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

type WriterInner<N> =
    RunTaskInThreads<BytesInsertBatch<RowData>, BytesInsertBatch<()>, anyhow::Error, N>;

/// Shared core for both writers: ClickHouse client + task runner in a
/// `RunTaskInThreads`; format and columns are the only per-format inputs.
#[allow(clippy::too_many_arguments)]
fn build_writer_inner<N>(
    next_step: N,
    cluster_config: ClickhouseConfig,
    table: String,
    skip_write: bool,
    concurrency: &ConcurrencyConfig,
    storage_name: String,
    format: InsertFormat,
    columns: Option<&'static [&'static str]>,
) -> WriterInner<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    RunTaskInThreads::new(
        next_step,
        clickhouse_task_runner(
            Arc::new(ClickhouseClient::new(
                &cluster_config,
                &table,
                storage_name,
                format,
                columns,
            )),
            skip_write,
        ),
        concurrency,
        Some("clickhouse"),
    )
}

/// `ProcessingStrategy` impl delegating to the inner `RunTaskInThreads`.
macro_rules! impl_writer_delegate {
    ($ty:ident) => {
        impl<N> ProcessingStrategy<BytesInsertBatch<RowData>> for $ty<N>
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

            fn join(
                &mut self,
                timeout: Option<Duration>,
            ) -> Result<Option<CommitRequest>, StrategyError> {
                self.inner.join(timeout)
            }
        }
    };
}

/// Writer for the `JSONEachRow` wire format (the historical default).
pub struct JsonWriterStep<N> {
    inner: WriterInner<N>,
}

impl<N> JsonWriterStep<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    pub fn new(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
        storage_name: String,
    ) -> Self {
        JsonWriterStep {
            inner: build_writer_inner(
                next_step,
                cluster_config,
                table,
                skip_write,
                concurrency,
                storage_name,
                InsertFormat::JsonEachRow,
                None,
            ),
        }
    }
}

impl_writer_delegate!(JsonWriterStep);

/// Writer for the `RowBinary` wire format. `columns` is required: RowBinary is
/// positional, so the explicit column list maps wire order to the table's
/// columns (see `EAPItemRow::COLUMN_NAMES`).
pub struct RowBinaryWriterStep<N> {
    inner: WriterInner<N>,
}

impl<N> RowBinaryWriterStep<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    pub fn new(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
        storage_name: String,
        columns: &'static [&'static str],
    ) -> Self {
        RowBinaryWriterStep {
            inner: build_writer_inner(
                next_step,
                cluster_config,
                table,
                skip_write,
                concurrency,
                storage_name,
                InsertFormat::RowBinary,
                Some(columns),
            ),
        }
    }
}

impl_writer_delegate!(RowBinaryWriterStep);

/// A failed attempt, normalized so the retry loop does not care whether
/// ClickHouse answered with an error or never answered at all. `status` and
/// `timeout` are metric tags; `detail` is the human-readable cause.
struct FailedAttempt {
    status: String,
    /// Whether the deadline fired, as opposed to the connection failing
    /// outright. Distinguishes writes that stall from ones that are refused.
    timeout: bool,
    detail: String,
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
        let base_url = format!("{scheme}://{host}:{port}?insert_distributed_sync=1&decompress=1");
        let columns_clause = match columns {
            Some(cols) => format!(" ({})", cols.join(", ")),
            None => String::new(),
        };
        let query = format!(
            "INSERT INTO {table}{columns_clause} FORMAT {fmt}",
            fmt = format.as_str(),
        );

        let timeouts = get_clickhouse_write_client_timeouts(&storage_name);
        let client = Client::builder()
            .connect_timeout(timeouts.connect)
            .pool_idle_timeout(timeouts.pool_idle)
            .tcp_keepalive(timeouts.tcp_keepalive)
            .tcp_keepalive_interval(timeouts.tcp_keepalive_interval)
            .tcp_keepalive_retries(timeouts.tcp_keepalive_retries)
            .build()
            .expect("failed to build ClickHouse HTTP client");

        ClickhouseClient {
            client,
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

    /// One INSERT attempt. Both an HTTP error and a transport error come back
    /// as [`FailedAttempt`] so the retry loop can treat them the same.
    async fn send_once(&self, body: bytes::Bytes) -> Result<Response, FailedAttempt> {
        let res = self
            .client
            .post(self.build_url())
            .headers(self.headers.clone())
            // Re-read per attempt so the deadline can be retuned at runtime.
            .timeout(get_clickhouse_write_client_timeouts(&self.storage_name).request)
            .query(&[("query", &self.query)])
            .body(reqwest::Body::from(body))
            .send()
            .await;

        match res {
            Ok(response) if response.status() == reqwest::StatusCode::OK => Ok(response),
            Ok(response) => {
                let status = response.status().to_string();
                let detail = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "unknown error".to_string());
                Err(FailedAttempt {
                    status,
                    timeout: false,
                    detail,
                })
            }
            Err(e) => Err(FailedAttempt {
                status: "network_error".to_string(),
                timeout: e.is_timeout(),
                detail: e.to_string(),
            }),
        }
    }

    pub async fn send(&self, body: Vec<u8>) -> anyhow::Result<Response> {
        // Compress once — the body is identical across attempts and `Bytes` makes
        // each retry's clone a refcount bump. Drop frees the uncompressed copy
        // rather than holding it for the life of the retries.
        let body_bytes = bytes::Bytes::from(lz4_compress(&body));
        drop(body);

        let retry = get_clickhouse_write_retry(&self.storage_name);
        let attempts = retry.max_retries + 1;
        let mut attempt = 0;
        loop {
            let started = Instant::now();
            let failure = match self.send_once(body_bytes.clone()).await {
                Ok(response) => return Ok(response),
                Err(failure) => failure,
            };
            let elapsed_ms = started.elapsed().as_millis();

            let last = attempt + 1 == attempts;
            counter!(
                "rust_consumer.clickhouse_insert_error", 1,
                "status" => failure.status,
                "timeout" => failure.timeout,
                "attempt" => attempt + 1,
                "max_attempts" => attempts
            );

            if last {
                anyhow::bail!(
                    "error writing to clickhouse after {attempts} attempts ({elapsed_ms}ms on the final attempt): {}",
                    failure.detail
                );
            }

            tracing::warn!(
                "ClickHouse write failed (attempt {}/{attempts}) after {elapsed_ms}ms: status={}, error={}",
                attempt + 1,
                failure.status,
                failure.detail
            );

            tokio::time::sleep(retry.backoff(attempt)).await;
            attempt += 1;
        }
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
    use sentry_options::testing::override_options;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Once;
    use tokio::time::Instant;

    static INIT: Once = Once::new();
    fn init_options() {
        INIT.call_once(|| crate::init_sentry_options().unwrap());
    }

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

    /// End-to-end against a live ClickHouse: the only check that the natively
    /// compressed body is one the server accepts, covering the LZ4 framing, the
    /// CityHash checksum byte order and the `decompress=1` contract together.
    /// `send` bails on any non-200, so returning `Ok` is the assertion.
    #[tokio::test]
    async fn test_compressed_insert_against_live_clickhouse() {
        crate::testutils::initialize_python();
        let client = ClickhouseClient::new(
            &make_test_config(),
            "querylog_local",
            "test_storage".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        client
            .send(b"[]".to_vec())
            .await
            .expect("compressed INSERT rejected by ClickHouse");
    }

    #[test]
    fn test_url_with_runtime_config_override() {
        crate::testutils::initialize_python();
        init_options();
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
        let _guard = override_options(&[
            (
                "snuba",
                "clickhouse_load_balancing",
                json!({ "writer_v2_lb_test": "first_or_random" }),
            ),
            (
                "snuba",
                "clickhouse_load_balancing_first_offset",
                json!({ "writer_v2_lb_test": "1" }),
            ),
        ])
        .unwrap();

        let url = client.build_url();
        assert!(url.contains("load_balancing=first_or_random"));
        assert!(url.contains("load_balancing_first_offset=1"));
    }

    #[test]
    fn test_url_with_max_insert_block_size() {
        crate::testutils::initialize_python();
        init_options();
        let config = make_test_config();
        let client = ClickhouseClient::new(
            &config,
            "test_table",
            "writer_v2_block_size_test".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );
        let other_client = ClickhouseClient::new(
            &config,
            "test_table",
            "writer_v2_other_storage".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        // Default (key absent): no suffix.
        assert!(!client.build_url().contains("max_insert_block_size"));

        // Per-storage override at or above the ClickHouse default sets the suffix.
        {
            let _guard = override_options(&[(
                "snuba",
                "clickhouse_max_insert_block_size",
                json!({ "writer_v2_block_size_test": 2_000_000 }),
            )])
            .unwrap();
            assert!(client
                .build_url()
                .contains("&max_insert_block_size=2000000"));
            // A different storage isn't affected.
            assert!(!other_client.build_url().contains("max_insert_block_size"));
        }

        // Values below the ClickHouse default (1_048_449) are rejected.
        {
            let _guard = override_options(&[(
                "snuba",
                "clickhouse_max_insert_block_size",
                json!({ "writer_v2_block_size_test": 1_000_000 }),
            )])
            .unwrap();
            assert!(!client.build_url().contains("max_insert_block_size"));
        }

        // Exactly the default is accepted.
        {
            let _guard = override_options(&[(
                "snuba",
                "clickhouse_max_insert_block_size",
                json!({ "writer_v2_block_size_test": 1_048_449 }),
            )])
            .unwrap();
            assert!(client
                .build_url()
                .contains("&max_insert_block_size=1048449"));
        }
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
        init_options();
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_write_retry",
            json!({
                "retry_backoff_test": {
                    "initial_backoff_ms": 100.0,
                    "max_retries": 4,
                    "jitter_factor": 0.1
                }
            }),
        )])
        .unwrap();

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
            "retry_backoff_test".to_string(),
            InsertFormat::JsonEachRow,
            None,
        );

        let start_time = Instant::now();
        let result = client.send(b"test data".to_vec()).await;
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

    /// Accepts connections and never answers, reproducing a black-holed
    /// request. Returns the error, the elapsed time, and the connection count
    /// (i.e. attempts). Caller must hold an `override_options` guard for
    /// `storage_name`.
    async fn send_against_hung_server(
        storage_name: &str,
        format: InsertFormat,
        columns: Option<&[&str]>,
    ) -> (String, Duration, usize) {
        let conns = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let counter = conns.clone();
        tokio::spawn(async move {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                accepted.push(stream);
            }
        });

        let config = ClickhouseConfig {
            host: "127.0.0.1".to_string(),
            port: 9000,
            secure: false,
            http_port: port,
            user: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
        };
        let client = ClickhouseClient::new(
            &config,
            "test_table",
            storage_name.to_string(),
            format,
            columns,
        );

        let start_time = Instant::now();
        let result = client.send(b"test data".to_vec()).await;
        let elapsed = start_time.elapsed();

        (
            result.unwrap_err().to_string(),
            elapsed,
            conns.load(Ordering::SeqCst),
        )
    }

    /// Regression test for SNUBA-CCY, where a peer that never answers hung the
    /// write indefinitely. The connection count is the point: each timed-out
    /// attempt must be retried on a *fresh* connection, which is what
    /// `reqwest::retry` cannot do. Both formats, since they share one client.
    #[tokio::test]
    async fn test_hung_server_times_out_for_both_wire_formats() {
        crate::testutils::initialize_python();
        init_options();

        // One guard for both: the override replaces the whole dict.
        let retry = json!({ "initial_backoff_ms": 10.0, "max_retries": 2, "jitter_factor": 0.0 });
        let _guard = override_options(&[
            (
                "snuba",
                "clickhouse_write_client_timeouts",
                json!({
                    "hung_server_json_test": { "request_ms": 300 },
                    "hung_server_rowbinary_test": { "request_ms": 300 }
                }),
            ),
            (
                "snuba",
                "clickhouse_write_retry",
                json!({
                    "hung_server_json_test": retry,
                    "hung_server_rowbinary_test": retry
                }),
            ),
        ])
        .unwrap();

        let cases: [(&str, InsertFormat, Option<&[&str]>); 2] = [
            ("hung_server_json_test", InsertFormat::JsonEachRow, None),
            (
                "hung_server_rowbinary_test",
                InsertFormat::RowBinary,
                Some(&["organization_id", "timestamp"]),
            ),
        ];

        for (storage_name, format, columns) in cases {
            let (error_msg, elapsed, attempts) =
                send_against_hung_server(storage_name, format, columns).await;

            assert!(
                error_msg.contains("after 3 attempts"),
                "{format:?}: unexpected error: {error_msg}"
            );
            assert_eq!(
                attempts, 3,
                "{format:?}: each timed-out attempt must be retried on a new \
                 connection, so the hung server should see one per attempt"
            );
            // 3 x 300ms plus backoff; without the deadline it never returns.
            assert!(
                elapsed >= Duration::from_millis(900),
                "{format:?}: took {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(10),
                "{format:?}: took {elapsed:?}"
            );
        }
    }
}
