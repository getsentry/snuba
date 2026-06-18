//! Arroyo sink that inserts `eap_items` rows through the official `clickhouse`
//! crate's [`Inserter`].
//!
//! Shape: the processor hands this strategy a *typed* [`EAPItemRow`] per
//! message (wrapped in `BytesInsertBatch<Option<EAPItemRow>>`; `None` == a
//! skipped/empty message that still carries offsets). A long-lived actor task
//! owns one [`clickhouse::inserter::Inserter`] and writes each row the moment
//! it arrives — the wide struct is serialized into the inserter's byte buffer
//! and dropped immediately, so peak memory stays bounded by the buffer, not by
//! row count.
//!
//! The inserter owns the flush boundary: it is configured with our batch
//! settings (`with_max_rows`/`with_max_bytes` + `with_period`), and we drive it
//! with [`Inserter::commit`] (never `force_commit`). When `commit()` reports a
//! real flush (`Quantities.rows > 0`), the rows it flushed are durably in
//! ClickHouse; only then do we push *their* Kafka offsets downstream toward
//! `CommitOffsets`. A failed flush is never pushed, so its offsets are not
//! committed and the batch replays on restart — the same durability barrier the
//! old writer had.

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
use crate::processors::eap_items::EAPItemRow;
use crate::runtime_config::{get_load_balancing_config, get_max_insert_block_size};
use crate::types::BytesInsertBatch;

/// Upper bound on typed rows in flight between `submit` and the actor. Kept
/// small (and independent of `max_batch_size`) so the channel never becomes a
/// `Vec<EAPItemRow>` accumulator — the inserter's byte buffer is the only place
/// a whole batch lives. When full, `submit` returns `MessageRejected` and
/// arroyo back-pressures upstream.
const WORK_CHANNEL_CAPACITY: usize = 1024;

/// Work handed from the strategy (sync) to the actor (async), in arrival order.
enum WorkItem {
    Row {
        // Boxed because `EAPItemRow` is wide (~96 columns); keeps the channel
        // item small and the enum cheap to move.
        row: Box<EAPItemRow>,
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
    /// A write/commit/end failed: the offsets it would have covered are
    /// discarded (never pushed) so the batch replays.
    Err(String),
}

/// Metadata accumulated for the current unflushed window. Rows themselves live
/// only in the inserter's byte buffer.
#[derive(Default)]
struct Acc {
    committable: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    rows_written: usize,
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

/// Take the accumulated window and send it downstream as a ready flush.
fn emit_ready(
    out_tx: &mpsc::UnboundedSender<FlushOutcome>,
    acc: &mut Acc,
    bytes: u64,
    rows: u64,
    elapsed: Duration,
) {
    let taken = std::mem::take(acc);
    let _ = out_tx.send(FlushOutcome::Ready(Box::new(ReadyFlush {
        committable: taken.committable,
        meta: taken.meta,
        bytes,
        rows,
        elapsed,
    })));
}

fn build_client(cfg: &ClickhouseConfig) -> clickhouse::Client {
    let scheme = if cfg.secure { "https" } else { "http" };
    clickhouse::Client::default()
        .with_url(format!("{}://{}:{}", scheme, cfg.host, cfg.http_port))
        .with_user(cfg.user.clone())
        .with_password(cfg.password.clone())
        .with_database(cfg.database.clone())
        // Mirror the old writer's URL params: synchronous distributed insert and
        // "treat binary strings bound for JSON columns as JSON text".
        .with_setting("insert_distributed_sync", "1")
        .with_setting("input_format_binary_read_json_as_string", "1")
}

fn make_inserter(
    client: &clickhouse::Client,
    table: &str,
    storage_name: &str,
    max_rows: u64,
    max_bytes: u64,
    max_batch_time: Duration,
) -> clickhouse::inserter::Inserter<EAPItemRow> {
    let lb = get_load_balancing_config(storage_name);
    let mut inserter = client
        .inserter::<EAPItemRow>(table)
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
async fn run_inserter_actor(
    config: ClickhouseConfig,
    table: String,
    storage_name: String,
    max_rows: u64,
    max_bytes: u64,
    max_batch_time: Duration,
    skip_write: bool,
    mut work_rx: mpsc::Receiver<WorkItem>,
    out_tx: mpsc::UnboundedSender<FlushOutcome>,
) {
    let client = build_client(&config);
    let mut inserter = make_inserter(
        &client,
        &table,
        &storage_name,
        max_rows,
        max_bytes,
        max_batch_time,
    );
    let mut acc = Acc::default();

    // The crate only evaluates `with_period` inside `commit()`, so we call
    // `commit()` on a tick. Ticking finer than `max_batch_time` makes the
    // period flush fire promptly instead of slipping a whole window.
    let tick_period = (max_batch_time / 4).max(Duration::from_millis(10));
    let mut interval = tokio::time::interval(tick_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // consume the immediate first tick

    // Bounds how often a skip-only window pushes offsets, so we don't emit a
    // downstream message every tick when no rows are flowing.
    let mut skip_deadline = Deadline::new(max_batch_time);

    macro_rules! on_flush_err {
        ($e:expr) => {{
            let _ = out_tx.send(FlushOutcome::Err($e.to_string()));
            // `write` panics if reused after an error — rebuild, and drop the
            // doomed window so its offsets are never pushed (→ replay).
            inserter = make_inserter(
                &client,
                &table,
                &storage_name,
                max_rows,
                max_bytes,
                max_batch_time,
            );
            acc = Acc::default();
            skip_deadline = Deadline::new(max_batch_time);
        }};
    }

    loop {
        tokio::select! {
            maybe = work_rx.recv() => match maybe {
                Some(WorkItem::Row { row, committable, meta }) => {
                    acc.merge(committable, meta);
                    acc.rows_written += 1;
                    if skip_write {
                        continue;
                    }
                    let start = SystemTime::now();
                    if let Err(e) = inserter.write(&*row).await {
                        on_flush_err!(e);
                        continue;
                    }
                    match inserter.commit().await {
                        Ok(q) if q.rows > 0 => {
                            let elapsed = start.elapsed().unwrap_or_default();
                            emit_ready(&out_tx, &mut acc, q.bytes, q.rows, elapsed);
                            skip_deadline = Deadline::new(max_batch_time);
                        }
                        Ok(_) => {}
                        Err(e) => on_flush_err!(e),
                    }
                }
                Some(WorkItem::Skip { committable, meta }) => acc.merge(committable, meta),
                None => break, // strategy dropped the sender: finalize below
            },
            _ = interval.tick() => {
                if skip_write {
                    if acc.has_offsets() && skip_deadline.has_elapsed() {
                        let rows = acc.rows_written as u64;
                        emit_ready(&out_tx, &mut acc, 0, rows, Duration::ZERO);
                        skip_deadline = Deadline::new(max_batch_time);
                    }
                    continue;
                }
                let start = SystemTime::now();
                match inserter.commit().await {
                    Ok(q) if q.rows > 0 => {
                        let elapsed = start.elapsed().unwrap_or_default();
                        emit_ready(&out_tx, &mut acc, q.bytes, q.rows, elapsed);
                        skip_deadline = Deadline::new(max_batch_time);
                    }
                    Ok(_) => {
                        // Period elapsed with nothing to flush (skip-only
                        // window): advance offsets so the consumer doesn't stall.
                        if acc.has_offsets() && acc.rows_written == 0 && skip_deadline.has_elapsed() {
                            emit_ready(&out_tx, &mut acc, 0, 0, Duration::ZERO);
                            skip_deadline = Deadline::new(max_batch_time);
                        }
                    }
                    Err(e) => on_flush_err!(e),
                }
            }
        }
    }

    // Finalize: flush the last partial window.
    if skip_write {
        if acc.has_offsets() {
            let rows = acc.rows_written as u64;
            emit_ready(&out_tx, &mut acc, 0, rows, Duration::ZERO);
        }
        return;
    }
    let start = SystemTime::now();
    match inserter.end().await {
        Ok(q) => {
            if acc.has_offsets() {
                let elapsed = start.elapsed().unwrap_or_default();
                emit_ready(&out_tx, &mut acc, q.bytes, q.rows, elapsed);
            }
        }
        Err(e) => {
            let _ = out_tx.send(FlushOutcome::Err(e.to_string()));
        }
    }
}

pub struct EapItemsInserterSink<N> {
    next_step: N,
    /// `None` once the stream has been closed (in `join`/`terminate`).
    work_tx: Option<mpsc::Sender<WorkItem>>,
    out_rx: mpsc::UnboundedReceiver<FlushOutcome>,
    actor: Option<tokio::task::JoinHandle<()>>,
    /// A flushed message that the next step rejected; retried before pulling more.
    message_carried_over: Option<Message<BytesInsertBatch<()>>>,
    commit_request_carried_over: Option<CommitRequest>,
    actor_done: bool,
}

impl<N> EapItemsInserterSink<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
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

        let (work_tx, work_rx) = mpsc::channel::<WorkItem>(WORK_CHANNEL_CAPACITY);
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
                    counter!(
                        "rust_consumer.clickhouse_insert_error",
                        1,
                        "status" => "insert_error",
                        "retried" => "false"
                    );
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

impl<N> ProcessingStrategy<BytesInsertBatch<Option<EAPItemRow>>> for EapItemsInserterSink<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
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
        message: Message<BytesInsertBatch<Option<EAPItemRow>>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<Option<EAPItemRow>>>> {
        // Downstream is backpressured, or we're shutting down: reject upstream too.
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }
        let Some(work_tx) = self.work_tx.as_ref() else {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        };
        // Reserve a slot first so we never destructure `message` on the reject path.
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
        // Drop the sender and abort the actor (abandons any open INSERT; the
        // server discards an un-`end()`ed insert, and un-pushed offsets replay).
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
                    tracing::warn!(
                        "inserter sink join timed out; partial batch left unacked (will replay)"
                    );
                    break;
                }
            }
            // Let the actor make progress on its runtime threads.
            std::thread::sleep(Duration::from_millis(1));
        }

        let next_commit = self.next_step.join(timeout)?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

impl<N> Drop for EapItemsInserterSink<N> {
    fn drop(&mut self) {
        // Abort the actor if it's still around so a dropped sink never flushes a
        // partial INSERT (whose offsets were never pushed) behind our back.
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

        let mut sink = EapItemsInserterSink::new(
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
