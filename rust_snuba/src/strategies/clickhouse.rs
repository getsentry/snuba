use std::time::Duration;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::types::{Message};
use rust_arroyo::utils::clickhouse_client::ClickhouseClient;
use crate::types::BytesInsertBatch;
use crate::config::ClickhouseConfig;
use tokio::task::JoinHandle;


pub struct ClickhouseWriterStep {
    next_step: Box<dyn ProcessingStrategy<()>>,
    clickhouse_client: ClickhouseClient,
    runtime: tokio::runtime::Runtime,
    skip_write: bool,
    handle: Option<JoinHandle<()>>
}

impl ClickhouseWriterStep {
    pub fn new<N>(next_step: N, cluster_config: ClickhouseConfig, table: String, skip_write: bool) -> Self
    where
        N: ProcessingStrategy<()> + 'static,
    {
        let hostname = cluster_config.host;
        let http_port = cluster_config.http_port;
        let database = cluster_config.database;

        ClickhouseWriterStep {
            next_step: Box::new(next_step),
            clickhouse_client: ClickhouseClient::new(&hostname, http_port, &table, &database),
            runtime: tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap(),
            skip_write,
            handle: None,
        }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        // if self.handle.is_some() && self.handle.as_ref().unwrap().is_finished() {
        //     let result = self.handle.take().unwrap();
        //     match self.runtime.block_on(result) {
        //         Ok(res) => {
        //             log::info!("Clickhouse response: {:?}", res);
        //         }
        //         Err(e) => {
        //             log::error!("Error writing to clickhouse: {}", e);
        //         }
        //     }
        // }
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<BytesInsertBatch>) -> Result<(), MessageRejected> {
        if self.handle.is_some() {
            return Err(MessageRejected);
        }
        let mut decoded_rows = vec![];
        for row in message.payload().rows {
            let decoded_row = String::from_utf8_lossy(&row);
            decoded_rows.push(decoded_row.to_string());
        }

        if !self.skip_write {
            let data = decoded_rows.join("\n");

            self.handle = Some(self.runtime.spawn(make_request(self.clickhouse_client.clone(), data)));

        }

        log::info!("Insert {} rows", message.payload().rows.len());

        self.next_step.submit(message.replace(()))
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

async fn make_request(client: ClickhouseClient, data: String) {
    let res = client.send(data).await;
    println!("Response: {:?}", res);
}
