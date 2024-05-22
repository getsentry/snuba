use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};
use rust_arroyo::gauge;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use std::mem;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;

use crate::runtime_config::get_str_config;
use crate::types::RowData;

const CLICKHOUSE_HTTP_CHUNK_SIZE: usize = 1_000_000;
const CHANNEL_CAPACITY: usize = 8_192;

pub struct BatchFactory {
    client: Client,
    url: String,
    query: String,
    handle: Handle,
}

#[allow(clippy::too_many_arguments)]
impl BatchFactory {
    pub fn new(
        hostname: &str,
        http_port: u16,
        table: &str,
        database: &str,
        concurrency: &ConcurrencyConfig,
        clickhouse_user: &str,
        clickhouse_password: &str,
        async_inserts: bool,
    ) -> Self {
        let mut headers = HeaderMap::with_capacity(5);
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        headers.insert(
            "X-Clickhouse-User",
            HeaderValue::from_str(clickhouse_user).unwrap(),
        );
        headers.insert(
            "X-ClickHouse-Key",
            HeaderValue::from_str(clickhouse_password).unwrap(),
        );
        headers.insert(
            "X-ClickHouse-Database",
            HeaderValue::from_str(database).unwrap(),
        );

        let mut query_params = String::new();
        query_params.push_str("load_balancing=in_order&insert_distributed_sync=1");

        let async_inserts_allowed = get_str_config("async_inserts_allowed").ok().flatten();
        if async_inserts == true && async_inserts_allowed == Some(1.to_string()) {
            query_params.push_str("&async_insert=1&wait_for_async_insert=1");
        }

        let url = format!("http://{hostname}:{http_port}?{query_params}");
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");

        let client = ClientBuilder::new()
            .default_headers(headers)
            .build()
            .unwrap();

        BatchFactory {
            client,
            url,
            query,
            handle: concurrency.handle(),
        }
    }

    pub fn new_batch(&self) -> HttpBatch {
        let (sender, receiver) = channel(CHANNEL_CAPACITY);

        let url = self.url.clone();
        let query = self.query.clone();
        let client = self.client.clone();

        let result_handle = self.handle.spawn(async move {
            while receiver.is_empty() && !receiver.is_closed() {
                // continously check on the receiver stream, only when it's
                // not empty do we write to clickhouse
                sleep(Duration::from_millis(800)).await;
            }

            if !receiver.is_empty() {
                // only make the request to clickhouse if there is data
                // being added to the receiver stream from the sender
                let res = client
                    .post(&url)
                    .query(&[("query", &query)])
                    .body(reqwest::Body::wrap_stream(ReceiverStream::new(receiver)))
                    .send()
                    .await?;

                if res.status() != reqwest::StatusCode::OK {
                    anyhow::bail!("error writing to clickhouse: {}", res.text().await?);
                }
            }

            Ok(())
        });

        let sender = Some(sender);
        let result_handle = Some(result_handle);

        HttpBatch {
            current_chunk: Vec::new(),
            num_rows: 0,
            num_bytes: 0,
            sender,
            result_handle,
        }
    }
}

pub struct HttpBatch {
    current_chunk: Vec<u8>,
    num_rows: usize,
    num_bytes: usize,
    sender: Option<Sender<Result<Vec<u8>, anyhow::Error>>>,
    result_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

impl HttpBatch {
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_bytes(&self) -> usize {
        self.num_bytes
    }

    pub fn write_rows(&mut self, data: &RowData) -> anyhow::Result<()> {
        if self.current_chunk.len() > CLICKHOUSE_HTTP_CHUNK_SIZE {
            self.flush_chunk()?;
        }

        self.num_rows += data.num_rows;
        self.num_bytes += data.encoded_rows.len();
        self.current_chunk.extend(&data.encoded_rows);
        Ok(())
    }

    #[inline]
    fn flush_chunk(&mut self) -> anyhow::Result<()> {
        if !self.current_chunk.is_empty() {
            // XXX: allocating small chunks of memory here and sending it across thread boundaries is
            // not very memory efficient, especially with jemalloc
            let chunk = mem::take(&mut self.current_chunk);
            if let Some(ref sender) = self.sender {
                sender.try_send(Ok(chunk))?;
                gauge!(
                    "rust_consumer.mpsc_channel_size",
                    (CHANNEL_CAPACITY - sender.capacity()) as u64
                );
            }
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<bool, anyhow::Error> {
        self.flush_chunk()?;
        // finish stream
        drop(self.sender.take());
        if let Some(handle) = self.result_handle.take() {
            handle.await??;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Drop for HttpBatch {
    fn drop(&mut self) {
        // in case the batch was not explicitly finished, send an error into the channel to abort
        // the request
        if let Some(ref mut sender) = self.sender {
            let _ = sender.try_send(Err(anyhow::anyhow!(
                "the batch got dropped without being finished explicitly"
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::{MockServer, POST};

    use super::*;

    #[test]
    fn test_write() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/").body("{\"hello\": \"world\"}\n");
            then.status(200).body("hi");
        });

        let concurrency = ConcurrencyConfig::new(1);
        let factory = BatchFactory::new(
            &server.host(),
            server.port(),
            "testtable",
            "testdb",
            &concurrency,
            "default",
            "",
            false,
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows(vec![
                br#"{"hello": "world"}"#.to_vec()
            ]))
            .unwrap();

        concurrency.handle().block_on(batch.finish()).unwrap();

        mock.assert();
    }

    #[test]
    fn test_write_async() {
        crate::testutils::initialize_python();
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/").body("{\"hello\": \"world\"}\n");
            then.status(200).body("hi");
        });

        let concurrency = ConcurrencyConfig::new(1);
        let factory = BatchFactory::new(
            &server.host(),
            server.port(),
            "testtable",
            "testdb",
            &concurrency,
            "default",
            "",
            true,
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows(vec![
                br#"{"hello": "world"}"#.to_vec()
            ]))
            .unwrap();

        concurrency.handle().block_on(batch.finish()).unwrap();

        mock.assert();
    }

    #[test]
    fn test_empty_batch() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).any_request();
            then.status(200).body("hi");
        });

        let concurrency = ConcurrencyConfig::new(1);
        let factory = BatchFactory::new(
            &server.host(),
            server.port(),
            "testtable",
            "testdb",
            &concurrency,
            "default",
            "",
            false,
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows([].to_vec()))
            .unwrap();

        concurrency.handle().block_on(batch.finish()).unwrap();

        mock.assert_hits(0);
    }

    #[test]
    fn test_drop_batch() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).any_request();
            then.status(200).body("hi");
        });

        let concurrency = ConcurrencyConfig::new(1);
        let factory = BatchFactory::new(
            &server.host(),
            server.port(),
            "testtable",
            "testdb",
            &concurrency,
            "default",
            "",
            false,
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows(vec![
                br#"{"hello": "world"}"#.to_vec()
            ]))
            .unwrap();

        // drop the batch -- it should not finish the request
        drop(batch);

        // ensure there has not been any HTTP request
        mock.assert_hits(0);
    }
}
