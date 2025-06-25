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
        let res = self
            .client
            .post(&self.url)
            .headers(self.headers.clone())
            .query(&[("query", &self.query)])
            .body(reqwest::Body::from(body))
            .send()
            .await?;

        if res.status() != reqwest::StatusCode::OK {
            anyhow::bail!("error writing to clickhouse: {}", res.text().await?);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn it_works() -> Result<(), reqwest::Error> {
        let config = ClickhouseConfig {
            host: std::env::var("CLICKHOUSE_HOST").unwrap_or("127.0.0.1".to_string()),
            port: 9000,
            secure: false,
            http_port: 8123,
            user: "default".to_string(),
            password: "default".to_string(),
            database: "default".to_string(),
        };
        let client: ClickhouseClient = ClickhouseClient::new(&config, "querylog_local");

        assert!(client.url.contains("load_balancing"));
        assert!(client.url.contains("insert_distributed_sync"));
        println!("running test");
        let res = client.send(b"[]").await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }
    #[test]
    fn chunked_body() {
        let mut data: Vec<u8> = vec![0; 1_000_000];

        assert_eq!(ClickhouseClient::chunked_body(&data).len(), 1);
        data.push(0);
        assert_eq!(ClickhouseClient::chunked_body(&data).len(), 2);
    }
}
