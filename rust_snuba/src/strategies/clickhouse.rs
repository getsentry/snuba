use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Response};
use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use rust_arroyo::types::Message;
use rust_arroyo::{counter, timer};

use crate::config::ClickhouseConfig;
use crate::types::BytesInsertBatch;

const CLICKHOUSE_HTTP_CHUNK_SIZE: usize = 1_000_000;

struct ClickhouseWriter {
    client: Arc<ClickhouseClient>,
    skip_write: bool,
}

impl ClickhouseWriter {
    pub fn new(client: ClickhouseClient, skip_write: bool) -> Self {
        ClickhouseWriter {
            client: Arc::new(client),
            skip_write,
        }
    }
}

impl TaskRunner<BytesInsertBatch, BytesInsertBatch, anyhow::Error> for ClickhouseWriter {
    fn get_task(
        &self,
        message: Message<BytesInsertBatch>,
    ) -> RunTaskFunc<BytesInsertBatch, anyhow::Error> {
        let skip_write = self.skip_write;
        let client = self.client.clone();

        Box::pin(async move {
            let insert_batch = message.payload();
            let write_start = SystemTime::now();

            if skip_write {
                tracing::info!("skipping write of {} rows", insert_batch.len());
            } else {
                tracing::debug!("performing write");

                let response = client
                    .send(insert_batch.encoded_rows())
                    .await
                    .map_err(RunTaskError::Other)?;

                tracing::debug!(?response);
                tracing::info!("Inserted {} rows", insert_batch.len());
            }

            let write_finish = SystemTime::now();

            if let Ok(elapsed) = write_finish.duration_since(write_start) {
                timer!("insertions.batch_write_ms", elapsed);
            }
            counter!("insertions.batch_write_msgs", insert_batch.len() as i64);
            insert_batch.record_message_latency();

            Ok(message)
        })
    }
}

pub struct ClickhouseWriterStep {
    inner: RunTaskInThreads<BytesInsertBatch, BytesInsertBatch, anyhow::Error>,
}

impl ClickhouseWriterStep {
    pub fn new<N>(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
    ) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let hostname = cluster_config.host;
        let http_port = cluster_config.http_port;
        let database = cluster_config.database;

        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(ClickhouseWriter::new(
                ClickhouseClient::new(&hostname, http_port, &table, &database),
                skip_write,
            )),
            concurrency,
            Some("clickhouse"),
        );

        ClickhouseWriterStep { inner }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch>,
    ) -> Result<(), SubmitError<BytesInsertBatch>> {
        self.inner.submit(message)
    }

    fn close(&mut self) {
        self.inner.close();
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
    pub fn new(hostname: &str, http_port: u16, table: &str, database: &str) -> ClickhouseClient {
        let mut headers = HeaderMap::with_capacity(3);
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        headers.insert(
            "X-ClickHouse-Database",
            HeaderValue::from_str(database).unwrap(),
        );

        let query_params = "load_balancing=in_order&insert_distributed_sync=1".to_string();
        let url = format!("http://{hostname}:{http_port}?{query_params}");
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");

        ClickhouseClient {
            client: Client::new(),
            headers,
            url,
            query,
        }
    }

    fn chunked_body(rows: &[u8]) -> Vec<Result<Box<[u8]>, std::io::Error>> {
        rows.chunks(CLICKHOUSE_HTTP_CHUNK_SIZE)
            .map(|chunk| Ok(Box::from(chunk)))
            .collect()
    }

    pub async fn send(&self, body: &[u8]) -> anyhow::Result<Response> {
        let chunks = Self::chunked_body(body);
        let stream = futures::stream::iter(chunks);

        let res = self
            .client
            .post(&self.url)
            .headers(self.headers.clone())
            .query(&[("query", &self.query)])
            .body(reqwest::Body::wrap_stream(stream))
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
        let client: ClickhouseClient = ClickhouseClient::new(
            &std::env::var("CLICKHOUSE_HOST").unwrap_or("127.0.0.1".to_string()),
            8123,
            "querylog_local",
            "default",
        );

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
