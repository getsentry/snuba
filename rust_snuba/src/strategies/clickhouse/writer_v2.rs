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
use crate::types::{BytesInsertBatch, RowData};

pub use super::client::NativeClickhouseClient;
use super::client::RetryConfig as NativeRetryConfig;

fn clickhouse_task_runner(
    client: Arc<HttpClickhouseClient>,
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

impl<N> ClickhouseWriterStep<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    pub fn new(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
    ) -> Self {
        let inner = RunTaskInThreads::new(
            next_step,
            clickhouse_task_runner(
                Arc::new(HttpClickhouseClient::new(&cluster_config.clone(), &table)),
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

/// Configuration for retry behavior when writing to ClickHouse.
pub struct RetryConfig {
    pub initial_backoff_ms: f64,
    pub max_retries: usize,
    pub jitter_factor: f64, // between 0 and 1
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

/// HTTP-based ClickHouse client using reqwest for JSONEachRow format.
/// This is the legacy client maintained for backward compatibility.
/// For new implementations using RowBinary format, use NativeClickhouseClient.
#[derive(Clone)]
pub struct HttpClickhouseClient {
    client: Client,
    headers: HeaderMap<HeaderValue>,
    url: String,
    query: String,
}

impl HttpClickhouseClient {
    pub fn new(config: &ClickhouseConfig, table: &str) -> HttpClickhouseClient {
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

        let query_params = "load_balancing=in_order&insert_distributed_sync=1".to_string();
        let url = format!("{scheme}://{host}:{port}?{query_params}");
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");

        HttpClickhouseClient {
            client: Client::new(),
            headers,
            url,
            query,
        }
    }

    pub async fn send(&self, body: Vec<u8>, retry_config: RetryConfig) -> anyhow::Result<Response> {
        // Convert to Bytes once for efficient cloning since sending the request
        // moves the body into the request body.
        let body_bytes = bytes::Bytes::from(body);

        for attempt in 0..=retry_config.max_retries {
            let res = self
                .client
                .post(&self.url)
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

// Type alias for backward compatibility
#[allow(dead_code)]
pub type ClickhouseClient = HttpClickhouseClient;

/// Task runner for the native ClickHouse client using RowBinary format.
/// This provides better performance than the HTTP/JSON path when typed rows are available.
fn native_clickhouse_task_runner(
    native_client: Arc<NativeClickhouseClient>,
    http_client: Arc<HttpClickhouseClient>,
    skip_write: bool,
) -> impl TaskRunner<BytesInsertBatch<RowData>, BytesInsertBatch<()>, anyhow::Error> {
    move |message: Message<BytesInsertBatch<RowData>>| -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        let skip_write = skip_write;
        let native_client = native_client.clone();
        let http_client = http_client.clone();

        Box::pin(async move {
            let (empty_message, insert_batch) = message.take();
            let batch_len = insert_batch.len();

            // Get typed_rows before calling take(), since take() moves typed_rows to the empty_batch
            let typed_rows = insert_batch.typed_rows().cloned();

            let (rows, empty_batch) = insert_batch.take();
            let encoded_rows = rows.into_encoded_rows();
            let num_bytes = encoded_rows.len();

            let write_start = SystemTime::now();

            // Skip empty batches (we configure Reduce to flush empty batches for commits)
            if batch_len == 0 {
                tracing::debug!(
                    "skipping write of empty payload ({} rows)",
                    batch_len
                );
            } else if skip_write {
                tracing::info!("skipping write of {} rows", batch_len);
            } else {
                tracing::debug!("performing write");

                // Prefer native client with typed rows, fall back to HTTP/JSON
                if let Some(typed_rows) = typed_rows {
                    tracing::debug!("using native RowBinary client for {} rows", typed_rows.len());
                    typed_rows
                        .insert_into(&native_client, NativeRetryConfig::default())
                        .await
                        .map_err(RunTaskError::Other)?;
                } else {
                    tracing::debug!("falling back to HTTP/JSON client (no typed rows available)");
                    http_client
                        .send(encoded_rows, RetryConfig::default())
                        .await
                        .map_err(RunTaskError::Other)?;
                }

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

/// ClickHouse writer step using the native client with RowBinary format.
/// Falls back to HTTP/JSON when typed rows are not available.
pub struct NativeClickhouseWriterStep<N> {
    inner: RunTaskInThreads<BytesInsertBatch<RowData>, BytesInsertBatch<()>, anyhow::Error, N>,
}

impl<N> NativeClickhouseWriterStep<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    pub fn new(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
    ) -> Self {
        let native_client = Arc::new(NativeClickhouseClient::new(&cluster_config, &table));
        let http_client = Arc::new(HttpClickhouseClient::new(&cluster_config, &table));

        let inner = RunTaskInThreads::new(
            next_step,
            native_clickhouse_task_runner(native_client, http_client, skip_write),
            concurrency,
            Some("clickhouse_native"),
        );

        NativeClickhouseWriterStep { inner }
    }
}

impl<N> ProcessingStrategy<BytesInsertBatch<RowData>> for NativeClickhouseWriterStep<N>
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[tokio::test]
    async fn it_works() -> Result<(), reqwest::Error> {
        let config = ClickhouseConfig {
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
            use_native_client: false,
        };
        println!("config: {:?}", config);
        let client: HttpClickhouseClient = HttpClickhouseClient::new(&config, "querylog_local");

        assert!(client.url.contains("load_balancing"));
        assert!(client.url.contains("insert_distributed_sync"));
        println!("running test");
        let res = client.send(b"[]".to_vec(), RetryConfig::default()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_with_exponential_backoff() {
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
            use_native_client: false,
        };

        let client = HttpClickhouseClient::new(&config, "test_table");

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
