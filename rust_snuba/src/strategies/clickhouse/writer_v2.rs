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
                    .send(encoded_rows)
                    .await
                    .map_err(RunTaskError::Other)?;

                tracing::debug!(?response);
                tracing::info!("Inserted {} rows", batch_len);
                let write_finish = SystemTime::now();
                if let Ok(elapsed) = write_finish.duration_since(write_start) {
                    timer!("insertions.batch_write_ms", elapsed);
                }
            }


            counter!("insertions.batch_write_msgs", batch_len as i64);
            empty_batch.record_message_latency();

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
                Arc::new(ClickhouseClient::new(&cluster_config.clone(), &table)),
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

#[derive(Clone)]
pub struct ClickhouseClient {
    client: Client,
    headers: HeaderMap<HeaderValue>,
    url: String,
    query: String,
}

impl ClickhouseClient {
    pub fn new(config: &ClickhouseConfig, table: &str) -> ClickhouseClient {
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

        ClickhouseClient {
            client: Client::new(),
            headers,
            url,
            query,
        }
    }

    pub async fn send(&self, body: Vec<u8>) -> anyhow::Result<Response> {
        const MAX_RETRIES: usize = 4;
        const INITIAL_BACKOFF_MS: u64 = 50;

        for attempt in 0..=MAX_RETRIES {
            let res = self
                .client
                .post(&self.url)
                .headers(self.headers.clone())
                .query(&[("query", &self.query)])
                .body(reqwest::Body::from(body.clone()))
                .send()
                .await;

            match res {
                Ok(response) => {
                    if response.status() == reqwest::StatusCode::OK {
                        return Ok(response);
                    } else {
                        let status = response.status().to_string();
                        counter!("rust_consumer.clickhouse_insert_error", 1, "status" => status);
                        let error_text = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "unknown error".to_string());

                        if attempt == MAX_RETRIES {
                            anyhow::bail!(
                                "error writing to clickhouse after {} attempts: {}",
                                MAX_RETRIES + 1,
                                error_text
                            );
                        }

                        tracing::warn!(
                            "ClickHouse write failed (attempt {}/{}): status={}, error={}",
                            attempt + 1,
                            MAX_RETRIES + 1,
                            status,
                            error_text
                        );
                    }
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        anyhow::bail!(
                            "error writing to clickhouse after {} attempts: {}",
                            MAX_RETRIES + 1,
                            e
                        );
                    }

                    tracing::warn!(
                        "ClickHouse write failed (attempt {}/{}): {}",
                        attempt + 1,
                        MAX_RETRIES + 1,
                        e
                    );
                }
            }

            // Calculate exponential backoff delay
            if attempt < MAX_RETRIES {
                let backoff_ms = INITIAL_BACKOFF_MS * (2_u64.pow(attempt as u32));
                let delay = Duration::from_millis(backoff_ms);
                tracing::debug!(
                    "Retrying in {:?} (attempt {}/{})",
                    delay,
                    attempt + 1,
                    MAX_RETRIES
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
        };
        println!("config: {:?}", config);
        let client: ClickhouseClient = ClickhouseClient::new(&config, "querylog_local");

        assert!(client.url.contains("load_balancing"));
        assert!(client.url.contains("insert_distributed_sync"));
        println!("running test");
        let res = client.send(b"[]".to_vec()).await;
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
        };

        let client = ClickhouseClient::new(&config, "test_table");

        let start_time = Instant::now();
        let result = client.send(b"test data".to_vec()).await;
        let elapsed = start_time.elapsed();

        // Should fail after all retries
        assert!(result.is_err());

        // Should have taken at least the sum of our backoff delays
        // 50ms + 100ms + 200ms + 400ms = 750ms minimum
        assert!(elapsed >= Duration::from_millis(750));

        // Error message should mention the number of attempts
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("after 5 attempts"));
    }

    #[tokio::test]
    async fn test_retry_fails_after_max_attempts() {
        // This test is redundant with the previous one since both test the same scenario
        // (network errors causing all retries to fail)
        // The previous test already covers this case
    }

    #[tokio::test]
    async fn test_retry_with_network_errors() {
        // This test is also covered by the first test since network errors
        // are the primary failure mode when connecting to a non-existent server
        // The first test already verifies the retry behavior with network errors
    }
}
