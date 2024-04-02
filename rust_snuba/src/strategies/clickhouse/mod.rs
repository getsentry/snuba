use std::cell::RefCell;
use std::time::{Duration, SystemTime};

use rust_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskError, RunTaskFunc, RunTaskInThreads, TaskRunner,
};
use rust_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use rust_arroyo::types::Message;
use rust_arroyo::{counter, timer};

use crate::strategies::clickhouse::batch::HttpBatch;
use crate::types::BytesInsertBatch;

pub mod batch;

struct ClickhouseWriter {}

impl ClickhouseWriter {
    pub fn new() -> Self {
        ClickhouseWriter {}
    }
}

impl TaskRunner<BytesInsertBatch<HttpBatch>, BytesInsertBatch<()>, anyhow::Error>
    for ClickhouseWriter
{
    fn get_task(
        &self,
        message: Message<BytesInsertBatch<HttpBatch>>,
    ) -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        Box::pin(async move {
            // XXX: gross hack to try_map the message while retaining the old value in http_batch
            let http_batch = RefCell::new(None);
            let message = message
                .try_map(|insert_batch| {
                    let (http_batch2, return_value) = insert_batch.take();
                    *http_batch.borrow_mut() = Some(http_batch2);
                    Ok::<_, ()>(return_value)
                })
                .unwrap();
            let http_batch = http_batch.into_inner().unwrap();
            let num_rows = http_batch.num_rows();
            let num_bytes = http_batch.num_bytes();

            let write_start = SystemTime::now();

            tracing::debug!("performing write");
            let skip_write = http_batch.finish().await.map_err(RunTaskError::Other)?;

            if skip_write {
                tracing::info!("skipping write of {} rows", num_rows);
            } else {
                tracing::info!("Inserted {} rows", num_rows);
            }

            let write_finish = SystemTime::now();

            if let Ok(elapsed) = write_finish.duration_since(write_start) {
                timer!("insertions.batch_write_ms", elapsed);
            }
            counter!("insertions.batch_write_msgs", num_rows as i64);
            counter!("insertions.batch_write_bytes", num_bytes as i64);
            message.payload().record_message_latency();

            Ok(message)
        })
    }
}

pub struct ClickhouseWriterStep {
    inner: RunTaskInThreads<BytesInsertBatch<HttpBatch>, BytesInsertBatch<()>, anyhow::Error>,
}

impl ClickhouseWriterStep {
    pub fn new<N>(next_step: N, concurrency: &ConcurrencyConfig) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
    {
        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(ClickhouseWriter::new()),
            concurrency,
            Some("clickhouse"),
        );

        ClickhouseWriterStep { inner }
    }
}

impl ProcessingStrategy<BytesInsertBatch<HttpBatch>> for ClickhouseWriterStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<HttpBatch>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<HttpBatch>>> {
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
