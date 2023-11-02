use crate::config::ClickhouseConfig;
use crate::types::BytesInsertBatch;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Response};
use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, SubmitError,
};
use rust_arroyo::types::Message;
use std::time::Duration;

struct ClickhouseWriter {
    client: ClickhouseClient,
    skip_write: bool,
}

impl ClickhouseWriter {
    pub fn new(client: ClickhouseClient, skip_write: bool) -> Self {
        ClickhouseWriter { client, skip_write }
    }
}

impl TaskRunner<BytesInsertBatch, BytesInsertBatch> for ClickhouseWriter {
    fn get_task(&self, message: Message<BytesInsertBatch>) -> RunTaskFunc<BytesInsertBatch> {
        let skip_write = self.skip_write;
        let client: ClickhouseClient = self.client.clone();
        Box::pin(async move {
            let len = message.payload().rows.len();
            let mut data = vec![];
            for row in message.payload().rows {
                data.extend(row);
                data.extend(b"\n");
            }

            if skip_write {
                log::info!("skipping write of {} rows", len);
                return Ok(message);
            }

            log::debug!("performing write");
            let response = client.send(data).await.unwrap();

            log::debug!("response: {:?}", response);
            log::info!("Inserted {} rows", len);
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
    url: String,
    headers: HeaderMap<HeaderValue>,
    table: String,
}
impl ClickhouseClient {
    pub fn new(hostname: &str, http_port: u16, table: &str, database: &str) -> ClickhouseClient {
        let mut client = ClickhouseClient {
            client: Client::new(),
            url: format!("http://{}:{}", hostname, http_port),
            headers: HeaderMap::new(),
            table: table.to_string(),
        };

        client
            .headers
            .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        client
            .headers
            .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        client.headers.insert(
            "X-ClickHouse-Database",
            HeaderValue::from_str(database).unwrap(),
        );
        client
    }

    pub async fn send(&self, body: Vec<u8>) -> Result<Response, anyhow::Error> {
        let res = self
            .client
            .post(self.url.clone())
            .headers(self.headers.clone())
            .body(body)
            .query(&[(
                "query",
                format!("INSERT INTO {} FORMAT JSONEachRow", self.table),
            )])
            .send()
            .await?;

        if res.status() != reqwest::StatusCode::OK {
            return Err(anyhow::anyhow!(
                "error writing to clickhouse: {}",
                res.text().await?
            ));
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

        println!("{}", "running test");
        let res = client.send(b"[]".to_vec()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }
}
