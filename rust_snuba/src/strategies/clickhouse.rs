use crate::config::ClickhouseConfig;
use crate::types::BytesInsertBatch;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Error, Response};
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::Message;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct ClickhouseWriterStep {
    next_step: Box<dyn ProcessingStrategy<BytesInsertBatch>>,
    clickhouse_client: ClickhouseClient,
    runtime: tokio::runtime::Runtime,
    skip_write: bool,
    concurrency: usize,
    handles: VecDeque<JoinHandle<Result<Message<BytesInsertBatch>, Error>>>,
    message_carried_over: Option<Message<BytesInsertBatch>>,
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

        ClickhouseWriterStep {
            next_step: Box::new(next_step),
            clickhouse_client: ClickhouseClient::new(&hostname, http_port, &table, &database),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
            skip_write,
            concurrency,
            handles: VecDeque::new(),
            message_carried_over: None,
        }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        if let Some(message) = self.message_carried_over.take() {
            if let Err(MessageRejected {
                message: transformed_message,
            }) = self.next_step.submit(message)
            {
                self.message_carried_over = Some(transformed_message);
                return None;
            }
        }

        while !self.handles.is_empty() {
            if let Some(front) = self.handles.front() {
                if front.is_finished() {
                    let handle = self.handles.pop_front().unwrap();
                    match self.runtime.block_on(handle) {
                        Ok(Ok(message)) => {
                            if let Err(MessageRejected {
                                message: transformed_message,
                            }) = self.next_step.submit(message)
                            {
                                self.message_carried_over = Some(transformed_message);
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
                    break;
                }
            }
        }

        self.next_step.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch>,
    ) -> Result<(), MessageRejected<BytesInsertBatch>> {
        if self.message_carried_over.is_some() {
            log::warn!("carried over message, rejecting subsequent messages");
            return Err(MessageRejected { message });
        }

        if self.handles.len() > self.concurrency {
            log::warn!("Reached max concurrency, rejecting message");
            return Err(MessageRejected { message });
        }

        let len = message.payload().rows.len();
        let mut data = vec![];
        for row in message.payload().rows {
            data.extend(row);
            data.extend(b"\n");
        }

        if !self.skip_write {
            log::debug!("performing write");

            let client: ClickhouseClient = self.clickhouse_client.clone();
            let handle = self.runtime.spawn(async move {
                let response = client.send(data).await.unwrap();
                log::debug!("response: {:?}", response);
                log::info!("Inserted {} rows", len);
                Ok(message)
            });

            self.handles.push_back(handle);
        } else {
            log::info!("skipping write of {} rows", len);
            if let Err(MessageRejected {
                message: transformed_message,
            }) = self.next_step.submit(message)
            {
                self.message_carried_over = Some(transformed_message);
            }
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
        client.headers.insert(
            "X-ClickHouse-Database",
            HeaderValue::from_str(database).unwrap(),
        );
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
        let client: ClickhouseClient =
            ClickhouseClient::new("localhost", 8123, "querylog_local", "default");

        println!("{}", "running test");
        let res = client.send(b"[]".to_vec()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }
}
