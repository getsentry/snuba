use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};
use sentry_arroyo::gauge;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use std::mem;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;

use crate::runtime_config::get_str_config;
use crate::types::RowData;

const CLICKHOUSE_HTTP_CHUNK_SIZE_BYTES: usize = 1_000_000;
const CHANNEL_CAPACITY: usize = 8_192;

pub struct BatchFactory {
    /// HTTP client for sending requests
    client: Client,
    /// URL for where to send requests
    url: String,
    /// Prepended query body
    query: String,
    /// Handle for thread pool to spawn new HTTP batch listeners
    handle: Handle,
}

#[allow(clippy::too_many_arguments)]
impl BatchFactory {
    ///
    /// A BatchFactory stores a set of connection parameters on creation and concurrency configuration.
    /// It provides a method (`new_batch`) which returns an orchestrator for physical
    /// writes to ClickHouse.
    ///
    pub fn new(
        hostname: &str,
        http_port: u16,
        table: &str,
        database: &str,
        concurrency: &ConcurrencyConfig,
        clickhouse_user: &str,
        clickhouse_password: &str,
        clickhouse_secure: bool,
        async_inserts: bool,
        batch_write_timeout: Option<Duration>,
        custom_envoy_request_timeout: Option<u64>,
    ) -> Self {
        let mut headers = HeaderMap::with_capacity(6);
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
        if let Some(custom_envoy_request_timeout) = custom_envoy_request_timeout {
            headers.insert(
                "x-envoy-upstream-rq-per-try-timeout-ms",
                HeaderValue::from_str(&custom_envoy_request_timeout.to_string()).unwrap(),
            );
        }

        let mut query_params = String::new();
        query_params.push_str("load_balancing=in_order&insert_distributed_sync=1");

        if async_inserts {
            let async_inserts_allowed = get_str_config("async_inserts_allowed").ok().flatten();
            if async_inserts_allowed == Some("1".to_string()) {
                query_params.push_str("&async_insert=1&wait_for_async_insert=1");
            }
        }

        let scheme = if clickhouse_secure { "https" } else { "http" };

        let url = format!("{scheme}://{hostname}:{http_port}?{query_params}");
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");

        let client = if let Some(timeout_duration) = batch_write_timeout {
            ClientBuilder::new()
                .default_headers(headers)
                .timeout(timeout_duration)
                .build()
                .unwrap()
        } else {
            ClientBuilder::new()
                .default_headers(headers)
                .build()
                .unwrap()
        };

        BatchFactory {
            client,
            url,
            query,
            handle: concurrency.handle(),
        }
    }

    ///
    /// new_batch creates a `HttpBatch` with its factory's parameters.
    ///
    /// The caller writes to ClickHouse by sending data through the `sender` field in
    /// the returned HttpBatch, then calling `result_handle.take()` to force the write to occur. If
    /// the ConcurrencyConfig on the `BatchFactory` allows, then multiple writer threads may be spawned
    /// to handle inserts.
    ///
    /// This is generally wrapped by `HttpBatch.write_rows()` to add data to an internal buffer,
    /// followed by `HttpBatch.finish()` to push that internal buffer data across a channel to
    /// a (hopefully free) receiver thread which will initiate a HTTP request to ClickHouse based
    /// on the connection paraemters supplied in `BatchFactory::new()`.
    ///
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
                let response = client
                    .post(&url)
                    .query(&[("query", &query)])
                    .body(reqwest::Body::wrap_stream(ReceiverStream::new(receiver)))
                    .send()
                    .await?;

                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await;
                    anyhow::bail!(
                        "bad response while inserting rows, status: {}, response body: {:?}",
                        status,
                        body
                    );
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

///
/// `HttpBatch` encapsulates the state of a single buffer for ClickHouse writes,
/// as well as handles for flushing data to a reader thread and a handle for initiating
/// a POST (insert) to ClickHouse
///
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

    ///
    /// write_rows writes rows to an internal buffer, up until hitting the point of
    /// CLICKHOUSE_HTTP_CHUNK_SIZE_BYTES bytes. When it hits that threshold, it will flush
    /// the existing buffer to the channel in `sender`.
    ///
    pub fn write_rows(&mut self, data: &RowData) -> anyhow::Result<()> {
        if self.current_chunk.len() > CLICKHOUSE_HTTP_CHUNK_SIZE_BYTES {
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

    ///
    /// finish flushes the existing in-memory buffer and then forces an attempt to write to
    /// ClickHouse on the thread created in `BatchFactory::new`
    ///
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
            false,
            None,
            None,
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
            false,
            true,
            None,
            None,
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
            false,
            None,
            None,
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
            false,
            None,
            None,
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
    #[should_panic]
    fn test_write_no_time() {
        crate::testutils::initialize_python();
        let server = MockServer::start();

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
            true,
            // pass in an unreasonably short timeout
            // which prevents the client request from reaching Clickhouse
            Some(Duration::from_millis(0)),
            None,
        );

        let mut batch = factory.new_batch();

        batch
            .write_rows(&RowData::from_encoded_rows(vec![
                br#"{"hello": "world"}"#.to_vec()
            ]))
            .unwrap();

        concurrency.handle().block_on(batch.finish()).unwrap();
    }

    #[test]
    fn test_write_enough_time() {
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
            false,
            true,
            // pass in a reasonable timeout
            Some(Duration::from_millis(1000)),
            None,
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
}
