use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Response};
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::Message;

use crate::config::ClickhouseConfig;
use crate::types::BytesInsertBatch;

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

impl TaskRunner<BytesInsertBatch, BytesInsertBatch> for ClickhouseWriter {
    fn get_task(&self, message: Message<BytesInsertBatch>) -> RunTaskFunc<BytesInsertBatch> {
        let skip_write = self.skip_write;
        let client = self.client.clone();

        Box::pin(async move {
            let rows = &message.payload().rows;

            let len = rows.len();
            let mut data = vec![];
            for row in rows {
                data.extend(row);
                data.extend(b"\n");
            }

            if skip_write {
                tracing::info!("skipping write of {} rows", len);
                return Ok(message);
            }

            tracing::debug!("performing write");
            let response = client.send(data).await.unwrap();

            tracing::debug!(?response);
            tracing::info!("Inserted {} rows", len);
            Ok(message)
        })
    }
}

pub struct ClickhouseWriterStep {
    inner: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
}

impl ClickhouseWriterStep {
    pub fn new<N>(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: usize,
    ) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let hostname = cluster_config.host;
        let http_port = cluster_config.http_port;
        let database = cluster_config.database;

        let inner = Box::new(RunTaskInThreads::new(
            next_step,
            Box::new(ClickhouseWriter::new(
                ClickhouseClient::new(&hostname, http_port, &table, &database),
                skip_write,
            )),
            concurrency,
            Some("clickhouse"),
        ));

        ClickhouseWriterStep { inner }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, InvalidMessage> {
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

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, InvalidMessage> {
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

        let url = format!("http://{hostname}:{http_port}");
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
            .body(body)
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

        println!("running test");
        let res = client.send(b"[]".to_vec()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }
}
