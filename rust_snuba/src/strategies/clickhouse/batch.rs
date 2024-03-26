use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use std::mem;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

const CLICKHOUSE_HTTP_CHUNK_SIZE: usize = 1_000_000;

pub struct BatchFactory {
    client: Client,
    url: String,
    query: String,
    handle: Handle,
}

impl BatchFactory {
    pub fn new(
        hostname: &str,
        http_port: u16,
        table: &str,
        database: &str,
        concurrency: &ConcurrencyConfig,
    ) -> Self {
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

    pub fn new_batch(&self) -> Batch {
        // this channel is effectively bounded due to max-batch-size and max-batch-time. it is hard
        // however to enforce any limit locally because it would mean that in the Drop impl of
        // Batch, the send may block or fail
        let (sender, receiver) = unbounded_channel();

        let url = self.url.clone();
        let query = self.query.clone();
        let client = self.client.clone();

        let result_handle = self.handle.spawn(async move {
            let res = client
                .post(&url)
                .query(&[("query", &query)])
                .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(
                    receiver,
                )))
                .send()
                .await?;

            if res.status() != reqwest::StatusCode::OK {
                anyhow::bail!("error writing to clickhouse: {}", res.text().await?);
            }

            Ok(())
        });

        Batch {
            current_chunk: Vec::new(),
            sender: Some(sender),
            result_handle: Some(result_handle),
        }
    }
}

pub struct Batch {
    current_chunk: Vec<u8>,
    sender: Option<UnboundedSender<Result<Vec<u8>, anyhow::Error>>>,
    result_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

impl Batch {
    pub fn write_rows(&mut self, rows: &[u8]) -> anyhow::Result<()> {
        if self.current_chunk.len() > CLICKHOUSE_HTTP_CHUNK_SIZE {
            self.flush_chunk()?;
        }

        self.current_chunk.extend(rows);
        Ok(())
    }

    #[inline]
    fn flush_chunk(&mut self) -> anyhow::Result<()> {
        if !self.current_chunk.is_empty() {
            // XXX: allocating small chunks of memory here and sending it across thread boundaries is
            // not very memory efficient, especially with jemalloc
            let chunk = mem::take(&mut self.current_chunk);
            self.sender.as_ref().unwrap().send(Ok(chunk))?;
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<(), anyhow::Error> {
        self.flush_chunk()?;
        // finish stream
        drop(self.sender.take());
        self.result_handle.take().unwrap().await??;
        Ok(())
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        // in case the batch was not explicitly finished, send an error into the channel to abort
        // the request
        if let Some(ref mut sender) = self.sender {
            let _ = sender.send(Err(anyhow::anyhow!(
                "the batch got dropped without being finished explicitly"
            )));
        }
    }
}
