//! Arroyo sink that inserts typed rows through the official `clickhouse` crate's
//! [`Inserter`]. Generic over the row type `R` (a `#[derive(Row)]` struct);
//! `eap_items` (`EAPItemRow`) is the first caller.
//!
//! A long-lived actor task owns one [`Inserter`], configured with our batch
//! settings, and writes each row on arrival. Durability barrier: a window's
//! Kafka offsets are pushed downstream only after `commit()` reports its rows
//! flushed. On a flush error the retained window is replayed through a fresh
//! inserter with backoff; if retries are exhausted the actor fail-stops, offsets
//! are never pushed, and the batch replays on restart. Wire format is plain
//! `RowBinary` with validation off (see `build_client`).

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use sentry_arroyo::utils::timing::Deadline;
use sentry_arroyo::{counter, timer};
use tokio::sync::mpsc;

use clickhouse::inserter::Quantities;

use crate::config::{BatchSizeCalculation, ClickhouseConfig};
use crate::options::{get_load_balancing_config, get_max_insert_block_size};
use crate::types::BytesInsertBatch;

/// Bound on rows in flight between `submit` and the actor; when full, `submit`
/// returns `MessageRejected` and arroyo back-pressures upstream.
const WORK_CHANNEL_CAPACITY: usize = 1024;

const MAX_INSERT_RETRIES: usize = 4;
const INITIAL_RETRY_BACKOFF_MS: u64 = 500;

/// Work handed from the strategy (sync) to the actor (async), in arrival order.
enum WorkItem<R> {
    Row {
        // Boxed to keep the channel item small (row structs can be wide).
        row: Box<R>,
        committable: Vec<(Partition, u64)>,
        meta: BytesInsertBatch<()>,
    },
    Skip {
        committable: Vec<(Partition, u64)>,
        meta: BytesInsertBatch<()>,
    },
}

/// A flush succeeded (or a skip-only window elapsed): these offsets + metadata
/// are safe to push downstream.
struct ReadyFlush {
    committable: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    bytes: u64,
    rows: u64,
    elapsed: Duration,
}

/// Result handed from the actor back to the strategy, drained in `poll`.
enum FlushOutcome {
    /// Boxed because [`ReadyFlush`] is much larger than the `Err` variant.
    Ready(Box<ReadyFlush>),
    /// A write/commit/end failed; its offsets are dropped so the batch replays.
    Err(String),
}

/// The current unflushed window: rows retained (bounded by `max_batch_size`) for
/// retry, plus the offsets/metadata they'll commit once flushed.
struct Acc<R> {
    rows: Vec<R>,
    committable: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
}

// Hand-written so the impl doesn't require `R: Default`.
impl<R> Default for Acc<R> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            committable: BTreeMap::new(),
            meta: BytesInsertBatch::default(),
        }
    }
}

impl<R> Acc<R> {
    fn merge(&mut self, committable: Vec<(Partition, u64)>, meta: BytesInsertBatch<()>) {
        for (partition, offset) in committable {
            let entry = self.committable.entry(partition).or_insert(offset);
            *entry = (*entry).max(offset);
        }
        self.meta.merge_metadata(meta);
    }

    fn has_offsets(&self) -> bool {
        !self.committable.is_empty()
    }
}

/// Push the accumulated window downstream and reset the accumulator. Returns
/// `false` (after sending `FlushOutcome::Err`) if the flush count doesn't match
/// the retained window; the caller must then stop the actor.
///
/// Enforcing `rows == acc.rows.len()` at runtime (not just `debug_assert!`)
/// guards against committing offsets for rows that aren't durable: we only emit
/// offsets for a window the inserter actually flushed. It holds by construction
/// today (we `commit()` after every `write()`, and a flush ends the whole
/// INSERT), but a future change could break it and silently lose data.
#[must_use]
fn emit_ready<R>(
    out_tx: &mpsc::UnboundedSender<FlushOutcome>,
    acc: &mut Acc<R>,
    bytes: u64,
    rows: u64,
    elapsed: Duration,
) -> bool {
    if rows as usize != acc.rows.len() {
        let _ = out_tx.send(FlushOutcome::Err(format!(
            "flush count ({}) != retained window ({}); refusing to commit \
             offsets for rows that may not be durable",
            rows,
            acc.rows.len(),
        )));
        return false;
    }
    acc.rows.clear();
    let committable = std::mem::take(&mut acc.committable);
    let meta = std::mem::take(&mut acc.meta);
    let _ = out_tx.send(FlushOutcome::Ready(Box::new(ReadyFlush {
        committable,
        meta,
        bytes,
        rows,
        elapsed,
    })));
    true
}

fn build_client(cfg: &ClickhouseConfig) -> clickhouse::Client {
    let scheme = if cfg.secure { "https" } else { "http" };
    clickhouse::Client::default()
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
        .with_setting("input_format_binary_read_json_as_string", "1")
}

fn make_inserter<R: clickhouse::Row>(
    client: &clickhouse::Client,
    table: &str,
    storage_name: &str,
    max_rows: u64,
    max_bytes: u64,
    max_batch_time: Duration,
) -> clickhouse::inserter::Inserter<R> {
    let lb = get_load_balancing_config(storage_name);
    let mut inserter = client
        .inserter::<R>(table)
        .with_max_rows(max_rows)
        .with_max_bytes(max_bytes)
        .with_period(Some(max_batch_time))
        .with_setting("load_balancing", lb.load_balancing);
    if let Some(first_offset) = lb.first_offset {
        inserter = inserter.with_setting("load_balancing_first_offset", first_offset);
    }
    if let Some(block_size) = get_max_insert_block_size(storage_name) {
        inserter = inserter.with_setting("max_insert_block_size", block_size.to_string());
    }
    inserter
}

/// Durably insert `rows` as a single INSERT, retrying transient failures with
/// jittered exponential backoff. Used to recover a window after the long-lived
/// inserter hit a write/commit error (which poisons it). Returns the inserted
/// `Quantities` on success, or the last error string after exhausting retries.
#[allow(clippy::too_many_arguments)]
async fn flush_window_with_retry<R>(
    client: &clickhouse::Client,
    table: &str,
    storage_name: &str,
    max_rows: u64,
    max_bytes: u64,
    max_batch_time: Duration,
    rows: &[R],
) -> Result<Quantities, String>
where
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync,
{
    if rows.is_empty() {
        return Ok(Quantities::ZERO);
    }
    let mut backoff = Duration::from_millis(INITIAL_RETRY_BACKOFF_MS);
    let mut last_err = String::from("unknown error");
    for attempt in 0..=MAX_INSERT_RETRIES {
        // Fresh inserter each attempt; end() flushes the whole window as one INSERT.
        let mut inserter = make_inserter::<R>(
            client,
            table,
            storage_name,
            max_rows,
            max_bytes,
            max_batch_time,
        );
        let mut write_err: Option<String> = None;
        for row in rows {
            if let Err(e) = inserter.write(row).await {
                write_err = Some(e.to_string());
                break;
            }
        }
        match write_err {
            None => match inserter.end().await {
                Ok(quantities) => return Ok(quantities),
                Err(e) => last_err = e.to_string(),
            },
            Some(e) => last_err = e,
        }
        if attempt < MAX_INSERT_RETRIES {
            counter!(
                "rust_consumer.clickhouse_insert_error",
                1,
                "status" => "insert_error",
                "retried" => "true"
            );
            tracing::warn!(
                "ClickHouse insert failed (attempt {}/{}): {}",
                attempt + 1,
                MAX_INSERT_RETRIES + 1,
                last_err
            );
            // ±10% jitter so consumers don't retry in lockstep.
            let jitter = 1.0 + (rand::random::<f64>() * 0.2 - 0.1);
            tokio::time::sleep(backoff.mul_f64(jitter)).await;
            backoff = backoff.saturating_mul(2);
        }
    }
    counter!(
        "rust_consumer.clickhouse_insert_error",
        1,
        "status" => "insert_error",
        "retried" => "false"
    );
    Err(last_err)
}

#[allow(clippy::too_many_arguments)]
async fn run_inserter_actor<R>(
    config: ClickhouseConfig,
    table: String,
    storage_name: String,
    max_rows: u64,
    max_bytes: u64,
    max_batch_time: Duration,
    skip_write: bool,
    mut work_rx: mpsc::Receiver<WorkItem<R>>,
    out_tx: mpsc::UnboundedSender<FlushOutcome>,
) where
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync,
{
    let client = build_client(&config);
    let mut inserter = make_inserter::<R>(
        &client,
        &table,
        &storage_name,
        max_rows,
        max_bytes,
        max_batch_time,
    );
    let mut acc = Acc::default();

    // The crate only evaluates `with_period` inside `commit()`, so tick finer
    // than `max_batch_time` to fire the period flush promptly.
    let tick_period = (max_batch_time / 4).max(Duration::from_millis(10));
    let mut interval = tokio::time::interval(tick_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // consume the immediate first tick

    // Bounds how often a skip-only window pushes offsets.
    let mut skip_deadline = Deadline::new(max_batch_time);

    // A write/commit error poisons the inserter: replay the retained window
    // through a fresh one with backoff, then carry on. If retries are exhausted,
    // fail-stop (return) rather than consume later rows — inserting them with
    // uncommitted offsets would duplicate on replay.
    macro_rules! recover_or_fail {
        ($start:expr) => {{
            match flush_window_with_retry(
                &client,
                &table,
                &storage_name,
                max_rows,
                max_bytes,
                max_batch_time,
                &acc.rows,
            )
            .await
            {
                Ok(quantities) => {
                    inserter = make_inserter::<R>(
                        &client,
                        &table,
                        &storage_name,
                        max_rows,
                        max_bytes,
                        max_batch_time,
                    );
                    let elapsed = $start.elapsed().unwrap_or_default();
                    if !emit_ready(
                        &out_tx,
                        &mut acc,
                        quantities.bytes,
                        quantities.rows,
                        elapsed,
                    ) {
                        return;
                    }
                    skip_deadline = Deadline::new(max_batch_time);
                }
                Err(e) => {
                    let _ = out_tx.send(FlushOutcome::Err(e));
                    return;
                }
            }
        }};
    }

    loop {
        tokio::select! {
            maybe = work_rx.recv() => match maybe {
                Some(WorkItem::Row { row, committable, meta }) => {
                    acc.merge(committable, meta);
                    if skip_write {
                        continue;
                    }
                    // Retain for retry, then write from the buffer.
                    acc.rows.push(*row);
                    let start = SystemTime::now();
                    if inserter.write(acc.rows.last().unwrap()).await.is_err() {
                        recover_or_fail!(start);
                        continue;
                    }
                    match inserter.commit().await {
                        Ok(q) if q.rows > 0 => {
                            let elapsed = start.elapsed().unwrap_or_default();
                            if !emit_ready(&out_tx, &mut acc, q.bytes, q.rows, elapsed) {
                                return;
                            }
                            skip_deadline = Deadline::new(max_batch_time);
                        }
                        Ok(_) => {}
                        Err(_) => recover_or_fail!(start),
                    }
                }
                Some(WorkItem::Skip { committable, meta }) => acc.merge(committable, meta),
                None => break, // strategy dropped the sender: finalize below
            },
            _ = interval.tick() => {
                if skip_write {
                    if acc.has_offsets() && skip_deadline.has_elapsed() {
                        if !emit_ready(&out_tx, &mut acc, 0, 0, Duration::ZERO) {
                            return;
                        }
                        skip_deadline = Deadline::new(max_batch_time);
                    }
                    continue;
                }
                let start = SystemTime::now();
                match inserter.commit().await {
                    Ok(q) if q.rows > 0 => {
                        let elapsed = start.elapsed().unwrap_or_default();
                        if !emit_ready(&out_tx, &mut acc, q.bytes, q.rows, elapsed) {
                            return;
                        }
                        skip_deadline = Deadline::new(max_batch_time);
                    }
                    Ok(_) => {
                        // Advance offsets for a skip-only window. Guard on
                        // `rows.is_empty()`: emitting while real rows are buffered
                        // would commit past their (lower) offsets and lose data.
                        if acc.has_offsets() && acc.rows.is_empty() && skip_deadline.has_elapsed() {
                            if !emit_ready(&out_tx, &mut acc, 0, 0, Duration::ZERO) {
                                return;
                            }
                            skip_deadline = Deadline::new(max_batch_time);
                        }
                    }
                    Err(_) => recover_or_fail!(start),
                }
            }
        }
    }

    // Finalize: flush the last partial window (with the same retry on error).
    if skip_write {
        if acc.has_offsets() {
            // Returning next regardless, so ignore the bool (Err already sent).
            let _ = emit_ready(&out_tx, &mut acc, 0, 0, Duration::ZERO);
        }
        return;
    }
    let start = SystemTime::now();
    let final_result = match inserter.end().await {
        Ok(quantities) => Ok(quantities),
        Err(_) => {
            flush_window_with_retry(
                &client,
                &table,
                &storage_name,
                max_rows,
                max_bytes,
                max_batch_time,
                &acc.rows,
            )
            .await
        }
    };
    match final_result {
        Ok(quantities) => {
            if acc.has_offsets() {
                let elapsed = start.elapsed().unwrap_or_default();
                // End of the actor, so ignore the bool (Err already sent).
                let _ = emit_ready(
                    &out_tx,
                    &mut acc,
                    quantities.bytes,
                    quantities.rows,
                    elapsed,
                );
            }
        }
        Err(e) => {
            let _ = out_tx.send(FlushOutcome::Err(e));
        }
    }
}

pub struct RowBinaryInserterSink<R, N> {
    next_step: N,
    /// `None` once the stream has been closed (in `join`/`terminate`).
    work_tx: Option<mpsc::Sender<WorkItem<R>>>,
    out_rx: mpsc::UnboundedReceiver<FlushOutcome>,
    actor: Option<tokio::task::JoinHandle<()>>,
    /// A flushed message that the next step rejected; retried before pulling more.
    message_carried_over: Option<Message<BytesInsertBatch<()>>>,
    commit_request_carried_over: Option<CommitRequest>,
    actor_done: bool,
}

impl<R, N> RowBinaryInserterSink<R, N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        next_step: N,
        cluster: ClickhouseConfig,
        table: String,
        storage_name: String,
        concurrency: &ConcurrencyConfig,
        max_batch_size: usize,
        max_batch_time: Duration,
        batch_size_calculation: BatchSizeCalculation,
        skip_write: bool,
    ) -> Self {
        let (max_rows, max_bytes) = match batch_size_calculation {
            BatchSizeCalculation::Rows => (max_batch_size as u64, u64::MAX),
            BatchSizeCalculation::Bytes => (u64::MAX, max_batch_size as u64),
        };

        let (work_tx, work_rx) = mpsc::channel::<WorkItem<R>>(WORK_CHANNEL_CAPACITY);
        let (out_tx, out_rx) = mpsc::unbounded_channel::<FlushOutcome>();

        let actor = concurrency.handle().spawn(run_inserter_actor(
            cluster,
            table,
            storage_name,
            max_rows,
            max_bytes,
            max_batch_time,
            skip_write,
            work_rx,
            out_tx,
        ));

        Self {
            next_step,
            work_tx: Some(work_tx),
            out_rx,
            actor: Some(actor),
            message_carried_over: None,
            commit_request_carried_over: None,
            actor_done: false,
        }
    }

    /// Retry a downstream message that was previously rejected.
    fn submit_carried_over(&mut self) -> Result<(), StrategyError> {
        if let Some(message) = self.message_carried_over.take() {
            match self.next_step.submit(message) {
                Ok(()) => {}
                Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                    self.message_carried_over = Some(message);
                }
                Err(SubmitError::InvalidMessage(e)) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Drain ready flushes from the actor and push them downstream. Stops on
    /// downstream backpressure (carry the message and retry next poll).
    fn drain_outcomes(&mut self) -> Result<(), StrategyError> {
        while self.message_carried_over.is_none() {
            match self.out_rx.try_recv() {
                Ok(FlushOutcome::Ready(ready)) => {
                    let ReadyFlush {
                        committable,
                        meta,
                        bytes,
                        rows,
                        elapsed,
                    } = *ready;
                    timer!("insertions.batch_write_ms", elapsed);
                    counter!("insertions.batch_write_bytes", bytes as i64);
                    counter!("insertions.batch_write_msgs", rows as i64);
                    meta.record_message_latency();
                    meta.emit_item_type_metrics();
                    let message = Message::new_any_message(meta, committable);
                    match self.next_step.submit(message) {
                        Ok(()) => {}
                        Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                            self.message_carried_over = Some(message);
                        }
                        Err(SubmitError::InvalidMessage(e)) => return Err(e.into()),
                    }
                }
                Ok(FlushOutcome::Err(e)) => {
                    // Metric already emitted in `flush_window_with_retry`; don't
                    // re-emit (would double-count). Just log and fail-stop.
                    tracing::error!("ClickHouse inserter flush failed: {}", e);
                    return Err(StrategyError::Other(e.into()));
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.actor_done = true;
                    break;
                }
            }
        }
        Ok(())
    }
}

impl<R, N> ProcessingStrategy<BytesInsertBatch<Option<R>>> for RowBinaryInserterSink<R, N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
    R: clickhouse::RowOwned + serde::Serialize + Send + Sync,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);
        self.submit_carried_over()?;
        self.drain_outcomes()?;
        Ok(self.commit_request_carried_over.take())
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<Option<R>>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<Option<R>>>> {
        // Downstream is backpressured, or we're shutting down: reject upstream too.
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }
        let Some(work_tx) = self.work_tx.as_ref() else {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        };
        // Reserve first so we never destructure `message` on the reject path.
        // `try_reserve` also fails if the actor closed the channel, so we never
        // accept a row it can't receive; a row lost to the reserve/send race just
        // replays (its offset is only committed via a downstream flush).
        let Ok(permit) = work_tx.try_reserve() else {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        };

        let committable: Vec<(Partition, u64)> = message.committable().collect();
        let (row, meta) = message.into_payload().take();
        let item = match row {
            Some(row) => WorkItem::Row {
                row: Box::new(row),
                committable,
                meta,
            },
            None => WorkItem::Skip { committable, meta },
        };
        permit.send(item);
        Ok(())
    }

    fn terminate(&mut self) {
        // Abort the actor; the server discards its un-`end()`ed INSERT and
        // un-pushed offsets replay.
        self.work_tx = None;
        if let Some(handle) = self.actor.take() {
            handle.abort();
        }
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        // Closing the sender makes the actor finalize (flush via `end()`), then
        // drop `out_tx` so `out_rx` disconnects.
        self.work_tx = None;
        let deadline = timeout.map(Deadline::new);

        let mut timed_out = false;
        loop {
            let commit_request = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);
            self.submit_carried_over()?;
            self.drain_outcomes()?;

            if self.actor_done && self.message_carried_over.is_none() {
                break;
            }
            if let Some(deadline) = &deadline {
                if deadline.has_elapsed() {
                    timed_out = true;
                    break;
                }
            }
            // Let the actor make progress on its runtime threads.
            std::thread::sleep(Duration::from_millis(1));
        }

        if timed_out {
            // Abort so the actor can't flush a window whose offsets we'll never
            // observe: an un-landed INSERT is cancelled (that window replays); a
            // flush that already landed replays as a duplicate (at-least-once, as
            // before).
            tracing::warn!(
                "inserter sink join timed out; aborting actor, partial batch will replay"
            );
            if let Some(handle) = self.actor.take() {
                handle.abort();
            }
        }
        // Final drain: commit any `Ready` produced right around the deadline.
        self.drain_outcomes()?;

        // Pass the *remaining* time so join can't block for up to 2x the budget.
        let next_commit = self.next_step.join(deadline.map(|d| d.remaining()))?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

impl<R, N> Drop for RowBinaryInserterSink<R, N> {
    fn drop(&mut self) {
        // Abort so a dropped sink never flushes a window behind our back.
        if let Some(handle) = self.actor.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use sentry_arroyo::processing::strategies::ProcessingStrategy;
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

    /// With `skip_write`, the actor never touches ClickHouse, so this exercises
    /// the full submit → actor → drain → downstream path (and `offset + 1`
    /// next-to-consume semantics) without a live cluster. Skipped (`None`) rows
    /// must still advance offsets, mirroring `flush_empty_batches`.
    #[test]
    fn skipped_messages_advance_offsets_without_clickhouse() {
        crate::testutils::initialize_python();

        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            committables: recorded.clone(),
        };
        let concurrency = ConcurrencyConfig::new(1);

        let mut sink = RowBinaryInserterSink::<EAPItemRow, _>::new(
            next_step,
            test_cluster(),
            "eap_items_1_local".to_string(),
            "test_storage".to_string(),
            &concurrency,
            1000,
            Duration::from_millis(50),
            BatchSizeCalculation::Rows,
            true, // skip_write
        );

        let partition = Partition::new(Topic::new("snuba-items"), 0);
        for offset in 0..5u64 {
            let message = Message::new_broker_message(
                BytesInsertBatch::<Option<EAPItemRow>>::default(),
                partition,
                offset,
                chrono::Utc::now(),
            );
            sink.submit(message).unwrap();
        }

        // join() closes the stream so the actor finalizes and flushes the
        // accumulated offsets, which we then drain downstream.
        sink.join(Some(Duration::from_secs(5))).unwrap();

        let recorded = recorded.lock().unwrap();
        let max_offset = recorded
            .iter()
            .filter_map(|c| c.get(&partition).copied())
            .max()
            .expect("offsets should have been pushed downstream");
        // Highest consumed offset is 4 → next-to-consume committable is 5.
        assert_eq!(max_offset, 5);
    }
}
