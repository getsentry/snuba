use std::time::Duration;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{Message};
use crate::types::BytesInsertBatch;
use crate::config::ClickhouseConfig;
use tokio::task::JoinHandle;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Error, Response};

pub struct ClickhouseWriterStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    clickhouse_client: ClickhouseClient,
    runtime: tokio::runtime::Runtime,
    skip_write: bool,
    handle: Option<JoinHandle<Result<Message<BytesInsertBatch>, reqwest::Error>>>,
    carried_over_message: Option<Message<BytesInsertBatch>>,
}

impl ClickhouseWriterStep {
    pub fn new<N>(next_step: N, cluster_config: ClickhouseConfig, table: String, skip_write: bool) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let hostname = cluster_config.host;
        let http_port = cluster_config.http_port;
        let database = cluster_config.database;

        ClickhouseWriterStep {
            next_step: Box::new(next_step),
            clickhouse_client: ClickhouseClient::new(&hostname, http_port, &table, &database),
            runtime: tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap(),
            skip_write,
            handle: None,
            carried_over_message: None,
        }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        if let Some(handle) = self.handle.take() {
            if handle.is_finished() {
                match self.runtime.block_on(handle) {
                    Ok(Ok(message)) => {
                        if let Err(MessageRejected{message: transformed_message}) = self.next_step.submit(message) {
                            self.carried_over_message = Some(transformed_message);
                        }
                    }
                    Ok(Err(e)) => {
                        log::error!("clickhouse error {}", e);
                    }
                    Err(e) => {
                        log::error!("the thread crashed {}", e);
                    }
                }
            } else {
                self.handle = Some(handle);
            }
        }

        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<BytesInsertBatch>) -> Result<(), MessageRejected<BytesInsertBatch>> {
        let len = message.payload().rows.len();

        if self.handle.is_some() {
            log::warn!("clickhouse writer is still busy, rejecting message");
            return Err(MessageRejected{message});
        }
        let mut data = vec![];
        for row in message.payload().rows {
            data.extend(row);
            data.extend(b"\n");
        }

        if !self.skip_write {
            log::debug!("performing write");

            let client = self.clickhouse_client.clone();
            self.handle = Some(self.runtime.spawn(async move {
                let response = client.send(data).await.unwrap();
                log::debug!("response: {:?}", response);
                log::info!("Inserted {} rows", len);
                Ok(message)
            }));
        }

        Ok(())
    }

    fn close(&mut self) {
        self.next_step.close();
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.next_step.join(timeout)
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
        client
            .headers
            .insert("X-ClickHouse-Database", HeaderValue::from_str(database).unwrap());
        client
    }

    pub async fn send(&self, body: Vec<u8>) -> Result<Response, Error> {
        self.client
            .post(self.url.clone())
            .headers(self.headers.clone())
            .body(body)
            .query(&[(
                "query",
                format!("INSERT INTO {} FORMAT JSONEachRow", self.table),
            )])
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn it_works() -> Result<(), reqwest::Error> {
        let client: ClickhouseClient = ClickhouseClient::new("localhost", 8123, "querylog_local", "default");

        println!("{}", "running test");
        let res = client.send(b"[]".to_vec()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }
}
