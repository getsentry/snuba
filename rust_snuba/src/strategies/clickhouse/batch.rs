use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};
use rust_arroyo::gauge;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use std::mem;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::types::RowData;

const CLICKHOUSE_HTTP_CHUNK_SIZE: usize = 1_000;
const CHANNEL_CAPACITY: usize = 4_096;

pub struct BatchFactory {
    client: Client,
    url: String,
    query: String,
    handle: Handle,
    skip_write: bool,
}

#[allow(clippy::too_many_arguments)]
impl BatchFactory {
    pub fn new(
        hostname: &str,
        http_port: u16,
        table: &str,
        database: &str,
        concurrency: &ConcurrencyConfig,
        skip_write: bool,
        clickhouse_user: &str,
        clickhouse_password: &str,
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

        let query_params = "load_balancing=in_order&insert_distributed_sync=1".to_string();
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
            skip_write,
        }
    }

    pub fn new_batch(&self) -> HttpBatch {
        let (sender, result_handle) = if self.skip_write {
            (None, None)
        } else {
            // this channel is effectively bounded due to max-batch-size and max-batch-time. it is hard
            // however to enforce any limit locally because it would mean that in the Drop impl of
            // Batch, the send may block or fail
            let (sender, receiver) = channel(CHANNEL_CAPACITY);

            let url = self.url.clone();
            let query = self.query.clone();
            let client = self.client.clone();

            let result_handle = self.handle.spawn(async move {
                let res = client
                    .post(&url)
                    .query(&[("query", &query)])
                    .body(reqwest::Body::wrap_stream(ReceiverStream::new(receiver)))
                    .send()
                    .await?;

                if res.status() != reqwest::StatusCode::OK {
                    anyhow::bail!("error writing to clickhouse: {}", res.text().await?);
                }

                Ok(())
            });

            (Some(sender), Some(result_handle))
        };

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
                dbg!(CHANNEL_CAPACITY - sender.capacity());
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
            false,
            "default",
            "",
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
            false,
            "default",
            "",
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

    #[test]
    fn test_skip_write() {
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
            true,
            "default",
            "",
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows(vec![
                br#"{"hello": "world"}"#.to_vec()
            ]))
            .unwrap();

        // finish the batch, but since we have skip_write=true, there should not be a http request
        concurrency.handle().block_on(batch.finish()).unwrap();

        // ensure there has not been any HTTP request
        mock.assert_hits(0);
    }

    #[test]
    fn test_drop_and_skip_write() {
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
            true,
            "default",
            "",
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows(vec![
                br#"{"hello": "world"}"#.to_vec()
            ]))
            .unwrap();

        // drop the batch -- it should not finish the request, but also we have skip_write=true
        drop(batch);

        // ensure there has not been any HTTP request
        mock.assert_hits(0);
    }
}
