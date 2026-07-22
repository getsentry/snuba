//! Arroyo sink that inserts typed rows through the official `clickhouse` crate's
//! [`Inserter`]. Generic over the row type `R` (a `#[derive(Row)]` struct);
//! `eap_items` (`EAPItemRow`) is the first caller.
//!
//! A pool of long-lived actor tasks (one per `clickhouse_concurrency` slot) each
//! own one [`Inserter`], configured with our batch settings, and write each row
//! on arrival. `submit` shards rows to an actor by Kafka partition, so every
//! partition's offsets are owned by exactly one actor and a single INSERT is
//! never split across actors — running the pool restores the parallel-insert
//! throughput a single serial inserter can't sustain. Durability barrier: a
//! window's Kafka offsets are pushed downstream only after `commit()` reports
//! its rows flushed. On a write/commit error the actor fail-stops (no in-process
//! retry): the window's offsets are never pushed, so the consumer restarts and
//! replays it from the last committed offset. Wire format is plain `RowBinary`
//! with validation off (see `build_client`).

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

use crate::config::{BatchSizeCalculation, ClickhouseConfig};
use crate::options::{get_insert_timeout, get_load_balancing_config, get_max_insert_block_size};
use crate::types::BytesInsertBatch;

/// Bound on rows in flight between `submit` and each actor (per-actor); when the
/// target actor's channel is full, `submit` returns `MessageRejected` and arroyo
/// back-pressures upstream.
const WORK_CHANNEL_CAPACITY: usize = 1024;

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

/// The current unflushed window: the offsets/metadata to commit once the
/// inserter flushes, plus a count of rows written since the last flush (used for
/// the flush-count safety check in `emit_ready`). We don't retain the rows
/// themselves — on a write/commit error we fail-stop and let Kafka replay.
#[derive(Default)]
struct Acc {
    pending_rows: u64,
    committable: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
}

impl Acc {
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
/// the rows written since the last flush; the caller must then stop the actor.
///
/// Enforcing `rows == acc.pending_rows` at runtime (not just `debug_assert!`)
/// guards against committing offsets for rows that aren't durable: we only emit
/// offsets for a window the inserter actually flushed. It holds by construction
/// today (we `commit()` after every `write()`, and a flush ends the whole
/// INSERT), but a future change could break it and silently lose data.
#[must_use]
fn emit_ready(
    out_tx: &mpsc::UnboundedSender<FlushOutcome>,
    acc: &mut Acc,
    bytes: u64,
    rows: u64,
    elapsed: Duration,
) -> bool {
    if rows != acc.pending_rows {
        let _ = out_tx.send(FlushOutcome::Err(format!(
            "flush count ({}) != rows written ({}); refusing to commit \
             offsets for rows that may not be durable",
            rows, acc.pending_rows,
        )));
        return false;
    }
    acc.pending_rows = 0;
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
    // Insert timeout (send + end), tunable per storage via
    // `clickhouse_insert_timeout_ms`; defaults to max_batch_time, 0 disables it.
    // Without a timeout a stalled ClickHouse or black-holed connection would block
    // the actor indefinitely; a timeout instead surfaces as a write/commit error
    // → fail-stop → Kafka replay.
    let insert_timeout = get_insert_timeout(storage_name, max_batch_time);
    let mut inserter = client
        .inserter::<R>(table)
        .with_timeouts(insert_timeout, insert_timeout)
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

    loop {
        tokio::select! {
            maybe = work_rx.recv() => match maybe {
                Some(WorkItem::Row { row, committable, meta }) => {
                    acc.merge(committable, meta);
                    if skip_write {
                        continue;
                    }
                    let start = SystemTime::now();
                    if let Err(e) = inserter.write(&*row).await {
                        let _ = out_tx.send(FlushOutcome::Err(e.to_string()));
                        return;
                    }
                    acc.pending_rows += 1;
                    match inserter.commit().await {
                        Ok(q) if q.rows > 0 => {
                            let elapsed = start.elapsed().unwrap_or_default();
                            if !emit_ready(&out_tx, &mut acc, q.bytes, q.rows, elapsed) {
                                return;
                            }
                            skip_deadline = Deadline::new(max_batch_time);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            let _ = out_tx.send(FlushOutcome::Err(e.to_string()));
                            return;
                        }
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
                        // `pending_rows == 0`: emitting while real rows are buffered
                        // would commit past their (lower) offsets and lose data.
                        if acc.has_offsets() && acc.pending_rows == 0 && skip_deadline.has_elapsed()
                        {
                            if !emit_ready(&out_tx, &mut acc, 0, 0, Duration::ZERO) {
                                return;
                            }
                            skip_deadline = Deadline::new(max_batch_time);
                        }
                    }
                    Err(e) => {
                        let _ = out_tx.send(FlushOutcome::Err(e.to_string()));
                        return;
                    }
                }
            }
        }
    }

    // Finalize: flush the last partial window via `end()`.
    if skip_write {
        if acc.has_offsets() {
            // Returning next regardless, so ignore the bool (Err already sent).
            let _ = emit_ready(&out_tx, &mut acc, 0, 0, Duration::ZERO);
        }
        return;
    }
    let start = SystemTime::now();
    match inserter.end().await {
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
            let _ = out_tx.send(FlushOutcome::Err(e.to_string()));
        }
    }
}

pub struct RowBinaryInserterSink<R, N> {
    next_step: N,
    /// One work sender per actor, indexed by shard (partition % len). `None` once
    /// the stream has been closed (in `join`/`terminate`), which drops every
    /// sender and makes the actors finalize.
    work_txs: Option<Vec<mpsc::Sender<WorkItem<R>>>>,
    /// Shared by all actors (each holds a clone of the sender); disconnects only
    /// once every actor has dropped its clone.
    out_rx: mpsc::UnboundedReceiver<FlushOutcome>,
    actors: Vec<tokio::task::JoinHandle<()>>,
    /// A flushed message that the next step rejected; retried before pulling more.
    message_carried_over: Option<Message<BytesInsertBatch<()>>>,
    commit_request_carried_over: Option<CommitRequest>,
    actors_done: bool,
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

        // One actor per concurrency slot. All inserts previously funneled through
        // a single serial actor (one INSERT in flight at a time); a pool restores
        // the parallel-insert throughput of the writer it replaced, since the
        // runtime backing `concurrency` has that many worker threads.
        let num_actors = concurrency.concurrency.max(1);
        let (out_tx, out_rx) = mpsc::unbounded_channel::<FlushOutcome>();

        let mut work_txs = Vec::with_capacity(num_actors);
        let mut actors = Vec::with_capacity(num_actors);
        for _ in 0..num_actors {
            let (work_tx, work_rx) = mpsc::channel::<WorkItem<R>>(WORK_CHANNEL_CAPACITY);
            let actor = concurrency.handle().spawn(run_inserter_actor(
                cluster.clone(),
                table.clone(),
                storage_name.clone(),
                max_rows,
                max_bytes,
                max_batch_time,
                skip_write,
                work_rx,
                out_tx.clone(),
            ));
            work_txs.push(work_tx);
            actors.push(actor);
        }
        // Drop our own handle so `out_rx` disconnects exactly when the last actor
        // exits (used to detect all-actors-done in `join`, and unexpected death
        // in `drain_outcomes`).
        drop(out_tx);

        Self {
            next_step,
            work_txs: Some(work_txs),
            out_rx,
            actors,
            message_carried_over: None,
            commit_request_carried_over: None,
            actors_done: false,
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
                    counter!(
                        "rust_consumer.clickhouse_insert_error",
                        1,
                        "status" => "insert_error"
                    );
                    tracing::error!("ClickHouse inserter flush failed: {}", e);
                    return Err(StrategyError::Other(e.into()));
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.actors_done = true;
                    // Every actor's sender dropped. If we didn't initiate shutdown
                    // (work_txs still present), the actors exited without sending
                    // an Err — e.g. a panic. Surface an error so the consumer
                    // restarts and respawns them, rather than silently wedging with
                    // submit() rejecting every message on now-closed work channels.
                    if self.work_txs.is_some() {
                        return Err(StrategyError::Other(
                            "inserter actors exited unexpectedly without reporting an error".into(),
                        ));
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    /// Detect a *single* actor dying unexpectedly. `out_rx` only disconnects once
    /// *all* actors drop their senders, so one panicked actor wouldn't be caught
    /// there — but its closed work channel would make `submit` reject that
    /// partition forever, wedging the consumer. During normal operation actors
    /// loop until their sender is dropped, so any finished handle before shutdown
    /// (`work_txs` still present) means an unexpected exit; error out to restart.
    ///
    /// Skipped while a message is carried over: `drain_outcomes` doesn't read the
    /// channel then, so an actor that finished after sending `FlushOutcome::Err`
    /// still has that error queued. Reporting a generic exit here would mask the
    /// real ClickHouse error text and its `clickhouse_insert_error` metric. Once
    /// downstream unblocks, `drain_outcomes` surfaces the queued `Err`; a true
    /// panic (nothing queued) is caught by this check on the next drained poll.
    fn check_actors_alive(&self) -> Result<(), StrategyError> {
        if self.work_txs.is_none() || self.message_carried_over.is_some() {
            return Ok(());
        }
        if self.actors.iter().any(|h| h.is_finished()) {
            return Err(StrategyError::Other(
                "inserter actor exited unexpectedly without reporting an error".into(),
            ));
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
        // After draining (so a real flush error surfaces first), catch a single
        // actor that panicked without reporting an error.
        self.check_actors_alive()?;
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
        let Some(work_txs) = self.work_txs.as_ref() else {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        };

        let committable: Vec<(Partition, u64)> = message.committable().collect();

        // Shard by Kafka partition so every offset of a partition is owned by one
        // actor and committed in order. Splitting one partition's offsets across
        // actors could let a higher offset flush (and commit) before a lower one
        // is durable, losing the gap on restart.
        //
        // This is sound only because each message carries exactly one partition
        // (upstream `RunTaskInThreads` is 1-in→1-out over single-partition broker
        // messages): routing then maps a partition to the same actor every time.
        // A multi-partition message has no single safe home — routing it by one of
        // its partitions would send the others to the "wrong" actor and reopen the
        // split-flush gap — so assert the invariant rather than silently mis-route.
        // Empty committable → shard 0.
        debug_assert!(
            committable.windows(2).all(|w| w[0].0.index == w[1].0.index),
            "inserter sink expects single-partition messages; got {committable:?}",
        );
        let shard = committable
            .first()
            .map_or(0, |(p, _)| p.index as usize % work_txs.len());
        let work_tx = &work_txs[shard];

        // Reserve first so we never destructure `message` on the reject path.
        // `try_reserve` also fails if the actor closed the channel, so we never
        // accept a row it can't receive; a row lost to the reserve/send race just
        // replays (its offset is only committed via a downstream flush).
        let Ok(permit) = work_tx.try_reserve() else {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        };

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
        // Abort every actor; the server discards their un-`end()`ed INSERTs and
        // un-pushed offsets replay.
        self.work_txs = None;
        for handle in self.actors.drain(..) {
            handle.abort();
        }
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        // Closing the senders makes every actor finalize (flush via `end()`), then
        // drop its `out_tx` clone so `out_rx` disconnects once the last one exits.
        self.work_txs = None;
        let deadline = timeout.map(Deadline::new);

        let mut timed_out = false;
        loop {
            let commit_request = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);
            self.submit_carried_over()?;
            self.drain_outcomes()?;

            if self.actors_done && self.message_carried_over.is_none() {
                break;
            }
            if let Some(deadline) = &deadline {
                if deadline.has_elapsed() {
                    timed_out = true;
                    break;
                }
            }
            // Let the actors make progress on their runtime threads.
            std::thread::sleep(Duration::from_millis(1));
        }

        if timed_out {
            // Abort so an actor can't flush a window whose offsets we'll never
            // observe: an un-landed INSERT is cancelled (that window replays); a
            // flush that already landed replays as a duplicate (at-least-once, as
            // before).
            tracing::warn!(
                "inserter sink join timed out; aborting actors, partial batch will replay"
            );
            for handle in self.actors.drain(..) {
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
        for handle in self.actors.drain(..) {
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

    /// With a pool of actors (`concurrency > 1`) and several partitions, every
    /// partition's offsets must still be committed — each partition is sharded to
    /// one actor by index, and all actors funnel their flushes into the shared
    /// outcome channel. Uses `skip_write` so no live ClickHouse is needed.
    #[test]
    fn multiple_actors_advance_offsets_for_all_partitions() {
        crate::testutils::initialize_python();

        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            committables: recorded.clone(),
        };
        // Three actors, four partitions → partitions 0 and 3 share an actor,
        // exercising the shard-collision path.
        let concurrency = ConcurrencyConfig::new(3);

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

        let topic = Topic::new("snuba-items");
        let partitions: Vec<Partition> = (0..4).map(|i| Partition::new(topic, i)).collect();
        for partition in &partitions {
            for offset in 0..5u64 {
                let message = Message::new_broker_message(
                    BytesInsertBatch::<Option<EAPItemRow>>::default(),
                    *partition,
                    offset,
                    chrono::Utc::now(),
                );
                sink.submit(message).unwrap();
            }
        }

        sink.join(Some(Duration::from_secs(5))).unwrap();

        let recorded = recorded.lock().unwrap();
        for partition in &partitions {
            let max_offset = recorded
                .iter()
                .filter_map(|c| c.get(partition).copied())
                .max()
                .unwrap_or_else(|| panic!("no offsets pushed for {partition:?}"));
            // Highest consumed offset is 4 → next-to-consume committable is 5.
            assert_eq!(max_offset, 5, "wrong committed offset for {partition:?}");
        }
    }
}
