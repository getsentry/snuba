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
use crate::runtime_config::get_load_balancing_config;
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
        storage_name: String,
    ) -> Self {
        let inner = RunTaskInThreads::new(
            next_step,
            clickhouse_task_runner(
                Arc::new(ClickhouseClient::new(
                    &cluster_config.clone(),
                    &table,
                    storage_name,
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
    pub fn new(config: &ClickhouseConfig, table: &str, storage_name: String) -> ClickhouseClient {
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

        let base_url = format!("{scheme}://{host}:{port}?insert_distributed_sync=1");
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");

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
        url
    }

    pub async fn send(&self, body: Vec<u8>, retry_config: RetryConfig) -> anyhow::Result<Response> {
        // Convert to Bytes once for efficient cloning since sending the request
        // moves the body into the request body.
        let body_bytes = bytes::Bytes::from(body);

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
        println!("config: {:?}", config);
        let client = ClickhouseClient::new(&config, "querylog_local", "test_storage".to_string());

        let url = client.build_url();
        assert!(url.contains("load_balancing=in_order"));
        assert!(url.contains("insert_distributed_sync"));
        println!("running test");
        let res = client.send(b"[]".to_vec(), RetryConfig::default()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }

    #[test]
    fn test_url_with_runtime_config_override() {
        crate::testutils::initialize_python();
        let config = make_test_config();
        let client = ClickhouseClient::new(&config, "test_table", "writer_v2_lb_test".to_string());

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

        let client = ClickhouseClient::new(&config, "test_table", "test_storage".to_string());

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
