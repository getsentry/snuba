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

use crate::strategies::clickhouse::batch::Batch;
use crate::types::BytesInsertBatch;

pub mod batch;

struct ClickhouseWriter {
    skip_write: bool,
}

impl ClickhouseWriter {
    pub fn new(skip_write: bool) -> Self {
        ClickhouseWriter { skip_write }
    }
}

impl TaskRunner<BytesInsertBatch<Batch>, BytesInsertBatch<()>, anyhow::Error> for ClickhouseWriter {
    fn get_task(
        &self,
        message: Message<BytesInsertBatch<Batch>>,
    ) -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        let skip_write = self.skip_write;

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

            if skip_write {
                // TODO: dont open connection at all
                tracing::info!("skipping write of {} rows", num_rows);
            } else {
                tracing::debug!("performing write");

                http_batch.finish().await.map_err(RunTaskError::Other)?;

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
    inner: RunTaskInThreads<BytesInsertBatch<Batch>, BytesInsertBatch<()>, anyhow::Error>,
}

impl ClickhouseWriterStep {
    pub fn new<N>(next_step: N, skip_write: bool, concurrency: &ConcurrencyConfig) -> Self
    where
        N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
    {
        let inner = RunTaskInThreads::new(
            next_step,
            Box::new(ClickhouseWriter::new(skip_write)),
            concurrency,
            Some("clickhouse"),
        );

        ClickhouseWriterStep { inner }
    }
}

impl ProcessingStrategy<BytesInsertBatch<Batch>> for ClickhouseWriterStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<Batch>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<Batch>>> {
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
