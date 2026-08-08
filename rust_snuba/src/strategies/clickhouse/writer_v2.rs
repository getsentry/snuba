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
    get_clickhouse_write_timeout, get_load_balancing_config, get_max_insert_block_size,
};
use crate::types::{BytesInsertBatch, RowData};

/// Attempts `reqwest` makes on top of the original request, matching the count
/// the hand-rolled loop this replaced used (5 total).
const MAX_RETRIES: u32 = 4;

/// Retry policy for ClickHouse INSERTs, which also emits the per-attempt error
/// metric as failures pass through the classifier.
///
/// A classifier has to be supplied explicitly. `for_host` builds on
/// `Builder::scoped`, which defaults to `Classifier::Never`, and the
/// client-wide default (`ProtocolNacks`) only matches HTTP/2 GOAWAY and
/// REFUSED_STREAM — impossible against ClickHouse's HTTP/1.1 endpoint. Left
/// alone, `reqwest` would retry nothing at all here.
///
/// Retries what the previous loop did: any non-200 response, and any transport
/// error. Timeouts are necessarily absent — the deadline is applied above the
/// retry layer, so a timed-out write never reaches this and is terminal.
///
/// The default token budget is kept. It caps retries at 20% extra load with a
/// floor of 10/s, which the previous loop had no equivalent of; a consumer
/// writing a batch every `max_batch_time` sits far below that floor, so the
/// budget only engages if something has gone badly wrong.
fn clickhouse_retry_policy(host: &str) -> reqwest::retry::Builder {
    reqwest::retry::for_host(host.to_string())
        .max_retries_per_request(MAX_RETRIES)
        .classify_fn(|req_rep| {
            // Resolve to an owned tag first: `retryable()` consumes `req_rep`,
            // so nothing may still be borrowing from it.
            let failure = match (req_rep.status(), req_rep.error()) {
                (Some(status), _) if status == reqwest::StatusCode::OK => None,
                (Some(status), _) => Some(status.to_string()),
                (None, Some(_)) => Some("network_error".to_string()),
                (None, None) => None,
            };

            match failure {
                Some(status) => {
                    counter!(
                        "rust_consumer.clickhouse_insert_error", 1,
                        "status" => status, "retried" => "true"
                    );
                    req_rep.retryable()
                }
                None => req_rep.success(),
            }
        })
}

/// Bounds the TCP connect and TLS handshake for a single attempt. Reaching
/// ClickHouse is an intra-cluster hop that normally completes in milliseconds;
/// this exists so a black-holed SYN fails into the retry loop quickly instead
/// of inheriting the kernel's multi-minute connect backoff.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// TCP keepalive: idle time before the kernel starts probing, then the spacing
/// and count of the probes. Detection lands at roughly `idle + interval *
/// retries`, so ~30s here.
///
/// This is the one stall detector whose failures `reqwest` will actually retry.
/// Its own `timeout`/`read_timeout` are applied above the retry layer, so a
/// deadline that fires there ends the write outright; a connection the kernel
/// declares dead instead surfaces as a transport error inside the stack, which
/// the classifier retries on a fresh connection. Against a black-holed flow —
/// a load balancer or NAT gateway dropping state without sending a RST, the
/// suspected shape of SNUBA-CCY — that is the difference between recovering and
/// failing the batch.
///
/// Hence the tight values. Left unset, interval and count come from the host's
/// `net.ipv4.tcp_keepalive_{intvl,probes}` sysctls, whose 75s x 9 defaults take
/// over 11 minutes. Probing this aggressively is safe: a peer's TCP stack
/// answers keepalives regardless of what the application is doing, so a slow
/// ClickHouse is never mistaken for a dead one.
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

                let result = client.send(encoded_rows).await;

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
        // retransmitting. The write deadline is set per request in `send`, so
        // it stays runtime-tunable; connect, keepalive and retries are
        // client-level and can only be configured here.
        //
        // No `read_timeout`: with the retry layer in play it would be a second
        // deadline over the same span as the per-request one, differing only in
        // resetting on each successful read — which an INSERT, whose response
        // arrives in one go, never exercises. One deadline is easier to reason
        // about than two that almost always fire together.
        //
        // `use_native_tls` is explicit because 0.13 made rustls the default and
        // feature unification compiles both backends into the tree.
        let client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(TCP_KEEPALIVE)
            .tcp_keepalive_interval(TCP_KEEPALIVE_INTERVAL)
            .tcp_keepalive_retries(TCP_KEEPALIVE_RETRIES)
            .retry(clickhouse_retry_policy(host))
            .use_native_tls()
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

    pub async fn send(&self, body: Vec<u8>) -> anyhow::Result<Response> {
        // Compress once. The encoded body is identical across attempts, so
        // paying the LZ4 cost per attempt would be wasted work.
        //
        // `Bytes` is what makes the retries possible at all: it becomes
        // `Body::Inner::Reusable`, whose `try_clone` hands `reqwest` a refcount
        // bump on the same allocation for each attempt. A streaming body would
        // hit `Inner::Streaming`, where `try_clone` returns `None` and
        // `clone_request` then declines to retry — silently, with no error.
        let body_bytes = bytes::Bytes::from(lz4_compress(&body));
        // Free the uncompressed buffer before handing off. With
        // `insert_distributed_sync=1` against a slow shard a write can hold its
        // in-flight slot for seconds — dragging `body` along kept ~1x the batch
        // size resident per slot for no reason.
        drop(body);

        // Re-read per write so the deadline can be retuned at runtime. This is
        // the deadline for the whole sequence, not one attempt: `reqwest`
        // applies it above its retry layer, so it does not reset between
        // attempts (see `DEFAULT_CLICKHOUSE_WRITE_TIMEOUT`).
        let write_timeout = get_clickhouse_write_timeout(&self.storage_name);
        let url = self.build_url();

        // A single call — `reqwest` performs the retries internally, per
        // `clickhouse_retry_policy`, which also emits the per-attempt metric.
        let res = self
            .client
            .post(&url)
            .headers(self.headers.clone())
            .query(&[("query", &self.query)])
            .timeout(write_timeout)
            .body(reqwest::Body::from(body_bytes))
            .send()
            .await;

        match res {
            Ok(response) if response.status() == reqwest::StatusCode::OK => Ok(response),
            Ok(response) => {
                // Retries are exhausted by the time a non-200 surfaces here;
                // the classifier already counted each attempt along the way.
                let status = response.status().to_string();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "unknown error".to_string());
                counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "false");
                anyhow::bail!(
                    "error writing to clickhouse after {} attempts: {}",
                    MAX_RETRIES + 1,
                    error_text
                );
            }
            Err(e) => {
                // A timeout means the deadline cut the write off, so it may
                // have landed server-side and there may have been no retry at
                // all; anything else is a transport failure that did exhaust
                // the retries. Retrying either is safe — ClickHouse
                // deduplicates identical insert blocks and nothing on this path
                // overrides `insert_deduplicate` — but they point at different
                // faults, so they get different `status` tags. A rise in
                // `timeout` means writes are stalling, `network_error` means
                // connections are being refused or reset.
                let (status, what) = if e.is_timeout() {
                    ("timeout", "timed out")
                } else {
                    ("network_error", "failed")
                };
                counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status, "retried" => "false");
                anyhow::bail!("clickhouse write {what} after {write_timeout:?}: {e}");
            }
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
        let res = client.send(b"[]".to_vec()).await;
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

    /// Binds a listener that accepts connections and immediately closes them,
    /// so every attempt fails fast at the transport layer. Returns how many
    /// connections the server saw, which is the attempt count.
    async fn count_attempts_against_failing_server(storage_name: &str) -> (usize, String) {
        let conns = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let counter = conns.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream);
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
            InsertFormat::JsonEachRow,
            None,
        );

        let err = client.send(b"test data".to_vec()).await.unwrap_err();
        (conns.load(Ordering::SeqCst), err.to_string())
    }

    /// `reqwest` retries transport failures, using the real policy and its
    /// default token budget.
    ///
    /// Worth asserting on the connection count rather than just the error: the
    /// retry path has two silent failure modes. A scoped builder defaults to
    /// `Classifier::Never`, and a non-reusable body makes `clone_request`
    /// decline — either would leave exactly one attempt with no error to show
    /// for it. Counting connections is what tells the two apart.
    #[tokio::test]
    async fn test_reqwest_retries_transport_failures() {
        crate::testutils::initialize_python();
        init_options();

        let (attempts, error_msg) =
            count_attempts_against_failing_server("retry_transport_test").await;

        assert_eq!(
            attempts,
            (MAX_RETRIES + 1) as usize,
            "expected 1 original + {MAX_RETRIES} retries, got {attempts} ({error_msg})"
        );
    }

    /// Binds a listener that accepts connections and never answers, and runs a
    /// full `send` against it. Reproduces a black-holed request: the peer is
    /// reachable, the body goes out, and no response ever comes back.
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
        let result = client.send(b"test data".to_vec()).await;
        let elapsed = start_time.elapsed();

        (
            result.unwrap_err().to_string(),
            elapsed,
            conns.load(Ordering::SeqCst),
        )
    }

    /// Regression test for SNUBA-CCY: a peer that accepts the connection and
    /// then never answers used to hang the write indefinitely, because
    /// `reqwest` applies no timeouts of its own. The deadline bounds it.
    ///
    /// Also pins the cumulative semantics. The deadline is applied above
    /// `reqwest`'s retry layer, so the stalled attempt consumes the whole
    /// budget and no retry is issued — exactly one connection, and the write
    /// ends at the deadline rather than a multiple of it. If a future change
    /// makes the deadline per-attempt again, the connection count moves and
    /// this fails.
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
            "clickhouse_write_timeout_ms",
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
                error_msg.contains("timed out"),
                "{format:?}: expected a timeout, got: {error_msg}"
            );
            assert_eq!(
                attempts, 1,
                "{format:?}: the deadline spans the retry sequence, so a stalled \
                 attempt should consume it without retrying"
            );
            // Bounded by the deadline, not hung. Without it this would not
            // have returned at all.
            assert!(
                elapsed >= Duration::from_millis(300),
                "{format:?}: took {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(10),
                "{format:?}: took {elapsed:?}"
            );
        }
    }
}
