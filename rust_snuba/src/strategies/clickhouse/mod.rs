use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use rust_arroyo::types::Message;
use rust_arroyo::{counter, timer};

use crate::config::ClickhouseConfig;
use crate::strategies::clickhouse::batch::BatchFactory;
use crate::types::BytesInsertBatch;

pub mod batch;

struct ClickhouseWriter {
    batch_factory: Arc<BatchFactory>,
    skip_write: bool,
}

impl ClickhouseWriter {
    pub fn new(batch_factory: BatchFactory, skip_write: bool) -> Self {
        ClickhouseWriter {
            batch_factory: Arc::new(batch_factory),
            skip_write,
        }
    }
}

impl TaskRunner<BytesInsertBatch, BytesInsertBatch, anyhow::Error> for ClickhouseWriter {
    fn get_task(
        &self,
        message: Message<BytesInsertBatch>,
    ) -> RunTaskFunc<BytesInsertBatch, anyhow::Error> {
        let skip_write = self.skip_write;
        let mut batch = self.batch_factory.new_batch();

        Box::pin(async move {
            let insert_batch = message.payload();
            let encoded_rows = insert_batch.encoded_rows();

            let write_start = SystemTime::now();

            // we can receive empty batches since we configure Reduce to flush empty batches, in
            // order to still be able to commit. in that case we want to skip the I/O to clickhouse
            // though.
            if encoded_rows.is_empty() {
                tracing::debug!(
                    "skipping write of empty payload ({} rows)",
                    insert_batch.len()
                );
            } else if skip_write {
                tracing::info!("skipping write of {} rows", insert_batch.len());
            } else {
                tracing::debug!("performing write");

                batch
                    .write_rows(&encoded_rows)
                    .map_err(RunTaskError::Other)?;

                let response = batch.finish().await.map_err(RunTaskError::Other)?;

                tracing::debug!(?response);
                tracing::info!("Inserted {} rows", insert_batch.len());
            }

            let write_finish = SystemTime::now();

            if let Ok(elapsed) = write_finish.duration_since(write_start) {
                timer!("insertions.batch_write_ms", elapsed);
            }
            counter!("insertions.batch_write_msgs", insert_batch.len() as i64);
            counter!("insertions.batch_write_bytes", encoded_rows.len() as i64);
            insert_batch.record_message_latency();

            Ok(message)
        })
    }
}

pub struct ClickhouseWriterStep {
    inner: RunTaskInThreads<BytesInsertBatch, BytesInsertBatch, anyhow::Error>,
}

impl ClickhouseWriterStep {
    pub fn new<N>(
        next_step: N,
        cluster_config: ClickhouseConfig,
        table: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
    ) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch> + 'static,
    {
        let hostname = cluster_config.host;
        let http_port = cluster_config.http_port;
        let database = cluster_config.database;

        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(ClickhouseWriter::new(
                BatchFactory::new(&hostname, http_port, &table, &database, concurrency),
                skip_write,
            )),
            concurrency,
            Some("clickhouse"),
        );

        ClickhouseWriterStep { inner }
    }
}

impl ProcessingStrategy<BytesInsertBatch> for ClickhouseWriterStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
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

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}
