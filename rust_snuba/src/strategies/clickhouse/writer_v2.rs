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
    get_clickhouse_request_timeout, get_load_balancing_config, get_max_insert_block_size,
};
use crate::types::{BytesInsertBatch, RowData};

/// Bounds the TCP connect and TLS handshake for a single attempt. Reaching
/// ClickHouse is an intra-cluster hop that normally completes in milliseconds;
/// this exists so a black-holed SYN fails into the retry loop quickly instead
/// of inheriting the kernel's multi-minute connect backoff.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// TCP keepalive: idle time before the kernel starts probing, then the spacing
/// and count of the probes. Detection lands at roughly `idle + interval *
/// retries`, so ~30s here.
///
/// Load balancers and NAT gateways drop idle flows, often without a RST. The
/// probes surface such a connection as a transport error, which fails the
/// attempt quickly instead of leaving it to sit until the write deadline —
/// cheaper than a timeout, and it frees the retry to dial a fresh connection
/// sooner.
///
/// Left unset, interval and count come from the host's
/// `net.ipv4.tcp_keepalive_{intvl,probes}` sysctls, whose 75s x 9 defaults take
/// over 11 minutes — useless against the stall this guards. Probing this
/// aggressively is safe: a peer's TCP stack answers keepalives regardless of
/// what the application is doing, so a slow ClickHouse is never mistaken for a
/// dead one.
const TCP_KEEPALIVE: Duration = Duration::from_secs(15);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
const TCP_KEEPALIVE_RETRIES: u32 = 3;

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

                let result = client.send(encoded_rows, RetryConfig::default()).await;

                // Record the latency on both paths. Timing only successes hid
                // exactly the writes worth seeing: a stalled INSERT never
                // reaches the success arm, so the slowest writes were dropped
                // from the timer and the metric stayed healthy while writes
                // hung. Filter to `outcome:success` for the old semantics.
                let outcome = if result.is_ok() { "success" } else { "error" };
                timer!(
                    "insertions.batch_write_ms",
                    write_start.elapsed(),
                    "outcome" => outcome
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

/// Retry schedule for [`ClickhouseClient::send`].
///
/// `reqwest::retry` (new in 0.13) covers most of what this loop does and adds a
/// token budget this has no equivalent of, so it is worth saying why it is not
/// used: **it cannot retry a timeout**, and retrying timeouts is the entire
/// reason this loop exists.
///
/// `reqwest` builds `total_timeout` and `read_timeout` in `execute_request` and
/// polls them in `PendingRequest::poll` *above* `in_flight` — and `in_flight` is
/// where its retry layer runs. A deadline firing there returns straight to the
/// caller, so the classifier never sees it. That makes its deadlines cumulative
/// over the whole sequence, and a stalled connection consumes the budget having
/// made a single attempt.
///
/// Measured, not inferred: against a server that accepts and never answers, one
/// policy makes 3 attempts with no deadline set and exactly 1 with one. The
/// write this module exists to rescue — a black-holed connection where a fresh
/// one would succeed — is precisely the write that arrangement stops retrying.
///
/// TCP keepalive narrows the gap but cannot close it: if the kernel spots the
/// dead connection you get a retryable transport error, and if it does not you
/// get a timeout. Every write that actually times out is by definition one
/// keepalive missed.
///
/// Revisit if upstream ever applies deadlines inside the retry layer. Body
/// replay and metrics both port over cleanly (the body is `Bytes`, so
/// `try_clone` succeeds at any size, and the classifier closure can emit
/// `rust_consumer.clickhouse_insert_error`).
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
        let base_url = format!("{scheme}://{host}:{port}?insert_distributed_sync=1&decompress=1");
        let columns_clause = match columns {
            Some(cols) => format!(" ({})", cols.join(", ")),
            None => String::new(),
        };
        let query = format!(
            "INSERT INTO {table}{columns_clause} FORMAT {fmt}",
            fmt = format.as_str(),
        );

        // `Client::new()` applies no timeouts whatsoever, which leaves a
        // request on a dead connection hanging until the kernel stops
        // retransmitting.
        //
        // `read_timeout` is the stall detector: it bounds the wait for the next
        // byte of the response, so a connection that goes quiet fails instead of
        // hanging. It is a client-level setting, so it snapshots the option at
        // startup; `send` additionally applies the same value as a per-request
        // total deadline, re-read each attempt, which both catches a response
        // that trickles forever and lets the deadline be lowered at runtime.
        //
        let client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(get_clickhouse_request_timeout(&storage_name))
            .tcp_keepalive(TCP_KEEPALIVE)
            .tcp_keepalive_interval(TCP_KEEPALIVE_INTERVAL)
            .tcp_keepalive_retries(TCP_KEEPALIVE_RETRIES)
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

    pub async fn send(&self, body: Vec<u8>, retry_config: RetryConfig) -> anyhow::Result<Response> {
        // Compress once before the retry loop — the encoded body is identical
        // across attempts, so paying the LZ4 cost per attempt would be wasted
        // work. `bytes::Bytes` makes the per-attempt clone cheap (refcount bump).
        let body_bytes = bytes::Bytes::from(lz4_compress(&body));
        // Free the uncompressed buffer before entering the retry loop. With
        // `insert_distributed_sync=1` against a slow shard the loop can hold
        // each in-flight slot for seconds — dragging `body` through it kept
        // ~1× the batch size resident per slot for no reason.
        drop(body);

        for attempt in 0..=retry_config.max_retries {
            let url = self.build_url();
            // Re-read per attempt so the deadline can be retuned at runtime.
            let request_timeout = get_clickhouse_request_timeout(&self.storage_name);
            let attempt_start = Instant::now();
            let res = self
                .client
                .post(&url)
                .headers(self.headers.clone())
                .query(&[("query", &self.query)])
                .timeout(request_timeout)
                .body(reqwest::Body::from(body_bytes.clone()))
                .send()
                .await;
            let elapsed_ms = attempt_start.elapsed().as_millis();

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
                                "error writing to clickhouse after {} attempts ({}ms on the final attempt): {}",
                                retry_config.max_retries + 1,
                                elapsed_ms,
                                error_text
                            );
                        }

                        counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "true");
                        tracing::warn!(
                            "ClickHouse write failed (attempt {}/{}) after {}ms: status={}, error={}",
                            attempt + 1,
                            retry_config.max_retries + 1,
                            elapsed_ms,
                            status,
                            error_text
                        );
                    }
                }
                Err(e) => {
                    // Distinguish a timeout from other transport failures. A
                    // connection error means the request demonstrably went
                    // nowhere; a timeout means the request was still
                    // outstanding at the deadline, so the insert may yet land
                    // server-side. Retrying either is safe: ClickHouse
                    // deduplicates identical insert blocks, and nothing on this
                    // path overrides `insert_deduplicate`. They do point at
                    // different faults though, so they get different `status`
                    // tags — a rise in `timeout` means connections are stalling,
                    // `network_error` means they are being refused or reset.
                    let status = if e.is_timeout() {
                        "timeout"
                    } else {
                        "network_error"
                    };

                    if attempt == retry_config.max_retries {
                        counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "false");
                        anyhow::bail!(
                            "error writing to clickhouse after {} attempts ({}ms on the final attempt): {}",
                            retry_config.max_retries + 1,
                            elapsed_ms,
                            e
                        );
                    }
                    counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "true");

                    tracing::warn!(
                        "ClickHouse write failed (attempt {}/{}) after {}ms: {}",
                        attempt + 1,
                        retry_config.max_retries + 1,
                        elapsed_ms,
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

    /// Binds a listener that accepts connections and then never answers, and
    /// runs a full `send` against it. Reproduces a black-holed request: the
    /// peer is reachable, the body goes out, and no response ever comes back.
    /// Returns the error, how long the call took, and how many connections the
    /// server saw — that last one being the attempt count.
    ///
    /// The caller must already hold an `override_options` guard covering
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
        let result = client
            .send(
                b"test data".to_vec(),
                RetryConfig {
                    initial_backoff_ms: 10.0,
                    max_retries: 2,
                    jitter_factor: 0.0,
                },
            )
            .await;
        let elapsed = start_time.elapsed();

        (
            result.unwrap_err().to_string(),
            elapsed,
            conns.load(Ordering::SeqCst),
        )
    }

    /// Regression test for SNUBA-CCY: a peer that accepts the connection and
    /// then never answers used to hang the write indefinitely, because
    /// `reqwest` applies no timeouts of its own. The retry loop was unreachable
    /// — the first attempt simply never returned.
    ///
    /// The connection count is the point of this test, and the reason the loop
    /// is hand-rolled rather than `reqwest::retry`. A timed-out attempt has to
    /// be *retried on a fresh connection*, and only a deadline the loop owns
    /// can do that: `reqwest` applies its own timeouts above its retry layer,
    /// so a deadline firing there ends the write with a single attempt. Three
    /// connections here is exactly the behaviour that arrangement cannot
    /// produce.
    ///
    /// Covers both wire formats. `JsonWriterStep` and `RowBinaryWriterStep`
    /// share one `ClickhouseClient` via `build_writer_inner`, so they cannot
    /// drift today — this pins that, so a future per-format client cannot
    /// quietly reintroduce an untimed one.
    #[tokio::test]
    async fn test_hung_server_times_out_for_both_wire_formats() {
        crate::testutils::initialize_python();
        init_options();

        // One guard covering both storages: the override replaces the whole
        // dict, so splitting these across tests would let them race.
        let _guard = override_options(&[(
            "snuba",
            "clickhouse_request_timeout_ms",
            json!({ "hung_server_json_test": 300, "hung_server_rowbinary_test": 300 }),
        )])
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
            // Three 300ms attempts plus ~30ms of backoff. Without the deadline
            // this call would not have returned at all within the bound.
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
