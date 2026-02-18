use std::time::{Duration, SystemTime};

use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use sentry_arroyo::{counter, timer};

use crate::config::ClickhouseConfig;
use crate::types::BytesInsertBatch;

struct RowBinaryTaskRunner<T> {
    client: clickhouse::Client,
    table: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for RowBinaryTaskRunner<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table: self.table.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> RowBinaryTaskRunner<T>
where
    T: clickhouse::Row + serde::Serialize + Send + Sync + 'static,
{
    fn new(config: &ClickhouseConfig, table: String) -> Self {
        let scheme = if config.secure { "https" } else { "http" };
        let client = clickhouse::Client::default()
            .with_url(format!("{}://{}:{}", scheme, config.host, config.http_port))
            .with_user(&config.user)
            .with_password(&config.password)
            .with_database(&config.database)
            .with_option("load_balancing", "in_order")
            .with_option("insert_distributed_sync", "1")
            .with_option("input_format_binary_read_json_as_string", "1");

        Self {
            client,
            table,
            _phantom: std::marker::PhantomData,
        }
    }
}

const INITIAL_BACKOFF_MS: f64 = 500.0;
const MAX_RETRIES: usize = 4;
const JITTER_FACTOR: f64 = 0.2;

impl<T> TaskRunner<BytesInsertBatch<Vec<T>>, BytesInsertBatch<()>, anyhow::Error>
    for RowBinaryTaskRunner<T>
where
    T: clickhouse::Row + serde::Serialize + Send + Sync + 'static,
{
    fn get_task(
        &self,
        message: Message<BytesInsertBatch<Vec<T>>>,
    ) -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        let client = self.client.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let (empty_message, insert_batch) = message.take();
            let batch_len = insert_batch.len();
            let (rows, empty_batch) = insert_batch.take();

            let write_start = SystemTime::now();

            if rows.is_empty() {
                tracing::debug!("skipping write of empty payload ({} rows)", batch_len);
            } else {
                tracing::debug!("performing row binary write");

                for attempt in 0..=MAX_RETRIES {
                    match write_rows(&client, &table, &rows).await {
                        Ok(()) => break,
                        Err(e) => {
                            if attempt == MAX_RETRIES {
                                counter!("rust_consumer.clickhouse_insert_error", 1, "status" => "row_binary_error", "retried" => "false");
                                return Err(RunTaskError::Other(anyhow::anyhow!(
                                    "error writing to clickhouse via RowBinary after {} attempts: {}",
                                    MAX_RETRIES + 1,
                                    e
                                )));
                            }
                            counter!("rust_consumer.clickhouse_insert_error", 1, "status" => "row_binary_error", "retried" => "true");
                            tracing::warn!(
                                "ClickHouse RowBinary write failed (attempt {}/{}): {}",
                                attempt + 1,
                                MAX_RETRIES + 1,
                                e
                            );

                            let backoff_ms =
                                INITIAL_BACKOFF_MS * (2_u64.pow(attempt as u32) as f64);
                            let jitter =
                                rand::random::<f64>() * JITTER_FACTOR - JITTER_FACTOR / 2.0;
                            let delay =
                                Duration::from_millis((backoff_ms * (1.0 + jitter)).round() as u64);
                            tokio::time::sleep(delay).await;
                        }
                    }
                }

                tracing::info!("Inserted {} rows via RowBinary", batch_len);
                let write_finish = SystemTime::now();
                if let Ok(elapsed) = write_finish.duration_since(write_start) {
                    timer!("insertions.batch_write_ms", elapsed);
                }
            }

            counter!("insertions.batch_write_msgs", batch_len as i64);
            empty_batch.record_message_latency();
            empty_batch.emit_item_type_metrics();

            Ok(empty_message.replace(empty_batch))
        })
    }
}

async fn write_rows<T: clickhouse::Row + serde::Serialize>(
    client: &clickhouse::Client,
    table: &str,
    rows: &[T],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert(table)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

pub struct ClickhouseRowBinaryWriterStep<T, N> {
    inner: RunTaskInThreads<BytesInsertBatch<Vec<T>>, BytesInsertBatch<()>, anyhow::Error, N>,
}

impl<T, N> ClickhouseRowBinaryWriterStep<T, N>
where
    T: clickhouse::Row + serde::Serialize + Send + Sync + 'static,
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    pub fn new(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        concurrency: &ConcurrencyConfig,
    ) -> Self {
        let task_runner = RowBinaryTaskRunner::<T>::new(&cluster_config, table);

        let inner = RunTaskInThreads::new(
            next_step,
            task_runner,
            concurrency,
            Some("clickhouse_row_binary"),
        );

        ClickhouseRowBinaryWriterStep { inner }
    }
}

impl<T, N> ProcessingStrategy<BytesInsertBatch<Vec<T>>> for ClickhouseRowBinaryWriterStep<T, N>
where
    T: clickhouse::Row + serde::Serialize + Send + Sync + 'static,
    N: ProcessingStrategy<BytesInsertBatch<()>>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<Vec<T>>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<Vec<T>>>> {
        self.inner.submit(message)
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}
