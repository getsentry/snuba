//! Arroyo strategy that inserts typed rows (`#[derive(Row)]`) through the
//! official `clickhouse` crate, **one INSERT per arroyo batch**, wrapped in
//! [`RunTaskInThreads`]. This mirrors the JSONEachRow [`ClickhouseWriterStep`]
//! (`writer_v2`), but hands ClickHouse typed RowBinary rows instead of
//! pre-encoded bytes; `eap_items` (`EAPItemRow`) is the first caller.
//!
//! Batching happens upstream in a [`Reduce`] that accumulates single processed
//! rows into a `Vec<R>`; each completed batch becomes one task here.
//! `RunTaskInThreads` runs up to `clickhouse_concurrency` inserts at once **but
//! drains their results strictly in submission order** (it only pops the front
//! of its handle queue), so Kafka offsets are committed in order and a batch's
//! offsets never advance before an earlier batch is durable. That in-order
//! completion barrier — not per-partition routing — is what keeps at-least-once
//! delivery safe under concurrency, exactly as `writer_v2` does.
//!
//! On a write/insert error the task returns `RunTaskError::Other`, which
//! `RunTaskInThreads` surfaces as a `StrategyError` (no in-process retry): the
//! batch's offsets are never committed, so the consumer restarts and replays
//! from the last committed offset. A panicked task is surfaced the same way via
//! its `JoinHandle`. Wire format is plain `RowBinary` with validation off (see
//! [`build_client`]).

use std::sync::Arc;
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
use crate::options::{get_insert_timeout, get_load_balancing_config, get_max_insert_block_size};
use crate::types::BytesInsertBatch;

fn build_client(cfg: &ClickhouseConfig, storage_name: &str) -> clickhouse::Client {
    let scheme = if cfg.secure { "https" } else { "http" };
    let mut client = clickhouse::Client::default()
        .with_url(format!("{}://{}:{}", scheme, cfg.host, cfg.http_port))
        .with_user(cfg.user.clone())
        .with_password(cfg.password.clone())
        .with_database(cfg.database.clone())
        // Validation off → plain `RowBinary`, so a `String` field can feed a
        // native `JSON` column via `input_format_binary_read_json_as_string`
        // (`RowBinaryWithNamesAndTypes` would reject the type). The crate still
        // emits the column list from the `Row` derive, so mapping stays correct.
        .with_validation(false)
        .with_setting("insert_distributed_sync", "1")
        .with_setting("input_format_binary_read_json_as_string", "1");

    // Per-storage tuning, previously applied on the long-lived inserter; these
    // are ClickHouse settings so they ride on every INSERT the client issues.
    let lb = get_load_balancing_config(storage_name);
    client = client.with_setting("load_balancing", lb.load_balancing);
    if let Some(first_offset) = lb.first_offset {
        client = client.with_setting("load_balancing_first_offset", first_offset);
    }
    if let Some(block_size) = get_max_insert_block_size(storage_name) {
        client = client.with_setting("max_insert_block_size", block_size.to_string());
    }
    client
}

/// Task run per arroyo batch: insert its rows via a single one-shot
/// `clickhouse::Insert`, then hand the offsets/metadata downstream.
fn clickhouse_row_task_runner<R>(
    client: Arc<clickhouse::Client>,
    table: String,
    storage_name: String,
    max_batch_time: Duration,
    skip_write: bool,
) -> impl TaskRunner<BytesInsertBatch<Vec<R>>, BytesInsertBatch<()>, anyhow::Error>
where
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync + 'static,
{
    move |message: Message<BytesInsertBatch<Vec<R>>>| -> RunTaskFunc<BytesInsertBatch<()>, anyhow::Error> {
        let client = client.clone();
        let table = table.clone();
        let storage_name = storage_name.clone();

        Box::pin(async move {
            let (empty_message, insert_batch) = message.take();
            let (rows, empty_batch) = insert_batch.take();
            let num_rows = rows.len();
            let num_bytes = empty_batch.num_bytes();
            let write_start = SystemTime::now();

            // Reduce flushes empty batches so offsets still advance; skip the I/O
            // for them (and when writes are disabled), mirroring writer_v2.
            if rows.is_empty() {
                tracing::debug!("skipping write of empty batch");
            } else if skip_write {
                tracing::info!("skipping write of {} rows", num_rows);
            } else {
                let insert_timeout = get_insert_timeout(&storage_name, max_batch_time);
                // Send + end timeout, tunable per storage via
                // `clickhouse_insert_timeout_ms` (0 disables); without it a stalled
                // ClickHouse or black-holed connection would block the task forever.
                let insert_result: anyhow::Result<()> = async {
                    let mut insert = client
                        .insert::<R>(&table)
                        .await?
                        .with_timeouts(insert_timeout, insert_timeout);
                    for row in &rows {
                        insert.write(row).await?;
                    }
                    insert.end().await
                }
                .await
                .map_err(anyhow::Error::from);

                if let Err(e) = insert_result {
                    counter!(
                        "rust_consumer.clickhouse_insert_error",
                        1,
                        "status" => "insert_error"
                    );
                    return Err(RunTaskError::Other(e));
                }

                if let Ok(elapsed) = write_start.elapsed() {
                    timer!("insertions.batch_write_ms", elapsed);
                }
                tracing::info!("Inserted {} rows", num_rows);
            }

            counter!("insertions.batch_write_bytes", num_bytes as i64);
            counter!("insertions.batch_write_msgs", num_rows as i64);
            empty_batch.record_message_latency();
            empty_batch.emit_item_type_metrics();

            Ok(empty_message.replace(empty_batch))
        })
    }
}

pub struct RowBinaryWriterStep<R, N> {
    inner: RunTaskInThreads<BytesInsertBatch<Vec<R>>, BytesInsertBatch<()>, anyhow::Error, N>,
}

impl<R, N> RowBinaryWriterStep<R, N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync + 'static,
{
    pub fn new(
        next_step: N,
        cluster: ClickhouseConfig,
        table: String,
        storage_name: String,
        skip_write: bool,
        concurrency: &ConcurrencyConfig,
        max_batch_time: Duration,
    ) -> Self {
        let client = Arc::new(build_client(&cluster, &storage_name));
        let inner = RunTaskInThreads::new(
            next_step,
            clickhouse_row_task_runner::<R>(client, table, storage_name, max_batch_time, skip_write),
            concurrency,
            Some("clickhouse_row_binary"),
        );
        RowBinaryWriterStep { inner }
    }
}

impl<R, N> ProcessingStrategy<BytesInsertBatch<Vec<R>>> for RowBinaryWriterStep<R, N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>>,
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<Vec<R>>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<Vec<R>>>> {
        self.inner.submit(message)
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use sentry_arroyo::types::{Partition, Topic};

    use crate::processors::eap_items::EAPItemRow;

    /// Records the committable of every message it receives.
    struct RecordingStep {
        committables: Arc<Mutex<Vec<BTreeMap<Partition, u64>>>>,
    }

    impl ProcessingStrategy<BytesInsertBatch<()>> for RecordingStep {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
        fn submit(
            &mut self,
            message: Message<BytesInsertBatch<()>>,
        ) -> Result<(), SubmitError<BytesInsertBatch<()>>> {
            let committable: BTreeMap<Partition, u64> = message.committable().collect();
            self.committables.lock().unwrap().push(committable);
            Ok(())
        }
        fn terminate(&mut self) {}
        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    fn test_cluster() -> ClickhouseConfig {
        ClickhouseConfig {
            host: "127.0.0.1".to_string(),
            port: 9000,
            secure: false,
            http_port: 8123,
            user: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
        }
    }

    /// With `skip_write` the task never touches ClickHouse, so this exercises the
    /// full submit → RunTaskInThreads → drain → downstream path (and `offset + 1`
    /// next-to-consume semantics) without a live cluster. Empty (skipped) batches
    /// must still advance offsets, mirroring `flush_empty_batches`.
    #[test]
    fn skipped_batches_advance_offsets_without_clickhouse() {
        crate::testutils::initialize_python();

        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            committables: recorded.clone(),
        };
        let concurrency = ConcurrencyConfig::new(2);

        let mut step = RowBinaryWriterStep::<EAPItemRow, _>::new(
            next_step,
            test_cluster(),
            "eap_items_1_local".to_string(),
            "test_storage".to_string(),
            true, // skip_write
            &concurrency,
            Duration::from_millis(50),
        );

        let partition = Partition::new(Topic::new("snuba-items"), 0);
        // Batches with next-to-consume offsets 5, 10, 15 (highest consumed 4/9/14).
        for offset in [5u64, 10, 15] {
            let batch = BytesInsertBatch::from_rows(Vec::<EAPItemRow>::new());
            let message = Message::new_broker_message(batch, partition, offset - 1, chrono::Utc::now());
            step.submit(message).unwrap();
            let _ = step.poll();
        }

        // join() drives RunTaskInThreads to completion and pushes results downstream.
        step.join(Some(Duration::from_secs(5))).unwrap();

        let recorded = recorded.lock().unwrap();
        let max_offset = recorded
            .iter()
            .filter_map(|c| c.get(&partition).copied())
            .max()
            .expect("offsets should have been pushed downstream");
        // Highest consumed offset is 14 → next-to-consume committable is 15.
        assert_eq!(max_offset, 15);
    }
}
