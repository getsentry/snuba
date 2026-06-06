//! Streaming write path for ClickHouse INSERTs.
//!
//! Replaces the `Reduce → ClickhouseWriterStep` pair for the RowBinary
//! path. Instead of accumulating the full uncompressed batch in a
//! `BytesInsertBatch<RowData>` and then handing it to a buffered writer
//! that compresses and sends in one shot, this strategy compresses rows
//! into ClickHouse-native LZ4 blocks as messages arrive and accumulates
//! the compressed `Bytes` in `chunks`. The encoded uncompressed bytes
//! are dropped per message, so we never hold a full uncompressed batch
//! in memory — peak resident set is ~0.3× the batch (compressed only).
//!
//! The HTTP POST is spawned at flush time, not at first-message time,
//! with the full compressed batch in hand. Earlier drafts streamed the
//! body live to overlap network with batch accumulation, but a network
//! failure mid-stream could cause the retry to fire against a partial
//! `retry_buf` (the strategy may still be adding chunks). ClickHouse
//! would accept that truncated body and return OK, dropping the rest
//! of the rows. Spawning at flush makes the body content immutable
//! before the first attempt, so retries always see the full batch.
//!
//! Concurrency is fixed at one in-flight batch — submits are rejected
//! while we're awaiting the previous batch's response. The existing
//! buffered path uses `RunTaskInThreads` for inter-batch concurrency;
//! we sacrifice that here for a much smaller resident set and a
//! simpler state machine.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use parking_lot::Mutex;
use reqwest::Response;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use sentry_arroyo::utils::timing::Deadline;
use sentry_arroyo::{counter, gauge, timer};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::streaming_lz4::StreamingLz4Compressor;
use super::writer_v2::{ClickhouseClient, RetryConfig};
use crate::types::{BytesInsertBatch, RowData};

/// State for the batch currently being accumulated. Holds the running
/// metadata plus the incremental compressor and the compressed-chunk
/// buffer that becomes the HTTP body at flush time.
struct PendingBatch {
    batch_start: Deadline,
    /// Running batch-size measurement under `compute_batch_size`. Drives
    /// the size-based flush decision in `poll`.
    batch_size: usize,
    /// Sum of `RowData::num_rows` across accumulated messages.
    num_rows: usize,
    /// Sum of uncompressed encoded byte sizes across accumulated
    /// messages. Reported as `insertions.batch_write_bytes` so the
    /// metric meaning matches the buffered writer.
    num_bytes: usize,
    /// Sum of compressed chunk sizes already in `chunks`. Tracked
    /// alongside the buffer so the resident-memory gauge can read it
    /// without locking + iterating `chunks` on every poll.
    compressed_bytes: usize,
    offsets: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    /// Wall-clock at the moment the first message of the batch arrived.
    /// Used to compute `insertions.batch_write_ms` on completion.
    write_start: SystemTime,
    /// Incremental ClickHouse-native LZ4 encoder. Fed one message's
    /// `encoded_rows` at a time, emits one `Bytes` per complete 1 MiB
    /// block; the final partial block is drained by `finish()` at flush.
    compressor: StreamingLz4Compressor,
    /// Compressed chunks accumulated for this batch. At flush time
    /// this is both the body source for the HTTP POST and the
    /// `retry_buf` passed to `send_streamed`, so the first attempt
    /// and retries replay the exact same bytes. Uses
    /// `parking_lot::Mutex` so neither side has to handle poisoning
    /// — the lock is held briefly and there's nothing useful to do
    /// if a holder panics anyway.
    chunks: Arc<Mutex<Vec<Bytes>>>,
}

/// How `try_drain_in_flight` should handle a not-yet-finished
/// HTTP handle. Keeps the join-time bounded wait distinct from
/// the non-blocking probe `poll` uses each tick.
enum DrainMode {
    /// Return immediately if the handle isn't finished yet.
    NonBlocking,
    /// Wait indefinitely (used when `join` was called with no timeout).
    BlockForever,
    /// Wait up to the given duration; abort the task on elapsed.
    BlockUpTo(Duration),
}

/// A batch whose body has been finalized and is awaiting the HTTP
/// response (or, for batches that never opened an HTTP request, ready
/// to be submitted downstream immediately).
struct InFlightBatch {
    handle: Option<JoinHandle<anyhow::Result<Response>>>,
    num_rows: usize,
    num_bytes: usize,
    /// Total compressed body size (== chunk buffer footprint). Held
    /// for the duration of the HTTP request; reported as the
    /// `resident_compressed_bytes` gauge while in flight and as
    /// `last_batch_compressed_bytes` once the response lands.
    compressed_bytes: usize,
    offsets: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    write_start: SystemTime,
}

pub struct StreamingClickhouseWriter<N> {
    next_step: N,
    client: Arc<ClickhouseClient>,
    skip_write: bool,
    runtime: Handle,
    max_batch_size: usize,
    max_batch_time: Duration,
    compute_batch_size: fn(&BytesInsertBatch<RowData>) -> usize,

    pending: Option<PendingBatch>,
    in_flight: Option<InFlightBatch>,

    /// Set when `next_step.submit` returned `MessageRejected`; retried
    /// on the next `poll`. Mirrors the carry-over pattern in `Reduce`.
    message_carried_over: Option<Message<BytesInsertBatch<()>>>,
    commit_request_carried_over: Option<CommitRequest>,
}

impl<N> StreamingClickhouseWriter<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        next_step: N,
        client: Arc<ClickhouseClient>,
        skip_write: bool,
        runtime: Handle,
        max_batch_size: usize,
        max_batch_time: Duration,
        compute_batch_size: fn(&BytesInsertBatch<RowData>) -> usize,
    ) -> Self {
        StreamingClickhouseWriter {
            next_step,
            client,
            skip_write,
            runtime,
            max_batch_size,
            max_batch_time,
            compute_batch_size,
            pending: None,
            in_flight: None,
            message_carried_over: None,
            commit_request_carried_over: None,
        }
    }

    fn ensure_pending(&mut self) -> &mut PendingBatch {
        self.pending.get_or_insert_with(|| PendingBatch {
            batch_start: Deadline::new(self.max_batch_time),
            batch_size: 0,
            num_rows: 0,
            num_bytes: 0,
            compressed_bytes: 0,
            offsets: BTreeMap::new(),
            meta: BytesInsertBatch::<()>::default(),
            write_start: SystemTime::now(),
            compressor: StreamingLz4Compressor::new(),
            chunks: Arc::new(Default::default()),
        })
    }

    /// Move `pending` → `in_flight`: drain the compressor's tail (if
    /// any), then — if any compressed bytes were produced — spawn the
    /// `send_streamed` task with the now-immutable chunk buffer as
    /// both the body source and the retry source. Must be called only
    /// when `in_flight` is `None` (the strategy keeps at most one
    /// batch in flight at a time).
    fn flush_pending(&mut self) {
        debug_assert!(self.in_flight.is_none());
        let Some(pending) = self.pending.take() else {
            return;
        };
        let PendingBatch {
            batch_start: _,
            batch_size: _,
            num_rows,
            num_bytes,
            mut compressed_bytes,
            offsets,
            meta,
            write_start,
            compressor,
            chunks,
        } = pending;

        if let Some(last) = compressor.finish() {
            compressed_bytes += last.len();
            chunks.lock().push(last);
        }

        // After this point nothing else writes to `chunks`. Both the
        // first attempt and retries inside `send_streamed` read from
        // the same `Arc<Mutex<Vec<Bytes>>>`, so the body content is
        // stable for the entire request lifetime.
        let handle = {
            let chunks_snapshot = chunks.lock();
            if chunks_snapshot.is_empty() {
                None
            } else {
                let body_chunks: Vec<Bytes> = chunks_snapshot.clone();
                drop(chunks_snapshot);
                let client = self.client.clone();
                let chunks_for_retry = chunks.clone();
                Some(self.runtime.spawn(async move {
                    let body_stream =
                        futures::stream::iter(body_chunks.into_iter().map(Ok::<_, io::Error>));
                    client
                        .send_streamed(body_stream, chunks_for_retry, RetryConfig::default())
                        .await
                }))
            }
        };

        self.in_flight = Some(InFlightBatch {
            handle,
            num_rows,
            num_bytes,
            compressed_bytes,
            offsets,
            meta,
            write_start,
        });
    }

    /// Try to drain the in-flight batch.
    ///
    /// Calls `Handle::block_on` to bridge from the synchronous arroyo
    /// strategy loop to the async tokio task. This is safe: arroyo
    /// invokes `poll`/`join` on the consumer thread, which is not
    /// itself running inside a tokio runtime — same pattern that
    /// `RunTaskInThreads` uses today.
    ///
    /// On success: emits per-batch metrics, builds an any-message with
    /// the merged meta + offsets, and submits it to `next_step`. A
    /// downstream `MessageRejected` is stashed in `message_carried_over`
    /// for `poll` to retry — we still consider `in_flight` drained.
    fn try_drain_in_flight(&mut self, mode: DrainMode) -> Result<(), StrategyError> {
        let Some(mut in_flight) = self.in_flight.take() else {
            return Ok(());
        };
        match in_flight.handle.take() {
            None => {}
            Some(mut handle) => match mode {
                DrainMode::NonBlocking => {
                    if !handle.is_finished() {
                        in_flight.handle = Some(handle);
                        self.in_flight = Some(in_flight);
                        return Ok(());
                    }
                    // is_finished() == true → block_on returns immediately.
                    match self.runtime.block_on(&mut handle) {
                        Ok(Ok(_response)) => {}
                        Ok(Err(e)) => return Err(StrategyError::Other(e.into())),
                        Err(e) => return Err(StrategyError::Other(Box::new(e))),
                    }
                }
                DrainMode::BlockForever => match self.runtime.block_on(&mut handle) {
                    Ok(Ok(_response)) => {}
                    Ok(Err(e)) => return Err(StrategyError::Other(e.into())),
                    Err(e) => return Err(StrategyError::Other(Box::new(e))),
                },
                DrainMode::BlockUpTo(max_wait) => {
                    // Pass `&mut handle` (JoinHandle is Unpin) so on
                    // timeout we still own the handle and can abort
                    // the task. Without the abort, the task would
                    // detach and keep its retry_buf live until it
                    // exhausts retries or the runtime drops.
                    let timeout_res = self
                        .runtime
                        .block_on(async { tokio::time::timeout(max_wait, &mut handle).await });
                    match timeout_res {
                        Ok(Ok(Ok(_response))) => {}
                        Ok(Ok(Err(e))) => return Err(StrategyError::Other(e.into())),
                        Ok(Err(e)) => return Err(StrategyError::Other(Box::new(e))),
                        Err(_elapsed) => {
                            handle.abort();
                            tracing::warn!(
                                "Streaming HTTP write exceeded {:?}; aborted in-flight task. Batch metadata not committed downstream — the next consumer instance will retry from the last committed offset.",
                                max_wait
                            );
                            return Ok(());
                        }
                    }
                }
            },
        }

        let write_finish = SystemTime::now();
        if let Ok(elapsed) = write_finish.duration_since(in_flight.write_start) {
            timer!("insertions.batch_write_ms", elapsed);
        }
        counter!("insertions.batch_write_bytes", in_flight.num_bytes as i64);
        counter!("insertions.batch_write_msgs", in_flight.num_rows as i64);
        // Per-batch input → output sizing. The pair lets dashboards
        // correlate ClickHouse insert load (compressed bytes on wire)
        // with the consumer-side memory peak we hold for that batch
        // — the resident gauge below samples the live side of the
        // same number.
        gauge!(
            "insertions.streaming_writer.last_batch_uncompressed_bytes",
            in_flight.num_bytes as u64
        );
        gauge!(
            "insertions.streaming_writer.last_batch_compressed_bytes",
            in_flight.compressed_bytes as u64
        );
        in_flight.meta.record_message_latency();
        in_flight.meta.emit_item_type_metrics();
        tracing::info!("Inserted {} rows (streamed)", in_flight.num_rows);

        let message = Message::new_any_message(in_flight.meta, in_flight.offsets);
        match self.next_step.submit(message) {
            Ok(()) => Ok(()),
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                self.message_carried_over = Some(message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(e)) => Err(e.into()),
        }
    }

    /// True if `pending` is at or past its size or time budget. Empty
    /// batches are eligible once the time budget elapses so commit-only
    /// flushes still propagate downstream (mirrors `Reduce` with
    /// `flush_empty_batches(true)`).
    fn pending_ready_to_flush(&self) -> bool {
        let Some(pending) = &self.pending else {
            return false;
        };
        pending.batch_size >= self.max_batch_size || pending.batch_start.has_elapsed()
    }

    fn try_resubmit_carried_over(&mut self) -> Result<(), StrategyError> {
        let Some(message) = self.message_carried_over.take() else {
            return Ok(());
        };
        match self.next_step.submit(message) {
            Ok(()) => Ok(()),
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                self.message_carried_over = Some(message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(e)) => Err(e.into()),
        }
    }
}

impl<N> ProcessingStrategy<BytesInsertBatch<RowData>> for StreamingClickhouseWriter<N>
where
    N: ProcessingStrategy<BytesInsertBatch<()>> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        self.try_resubmit_carried_over()?;

        // Drain a finished in-flight before considering whether to flush
        // pending — we only allow one batch in flight at a time.
        if self.message_carried_over.is_none() {
            self.try_drain_in_flight(DrainMode::NonBlocking)?;
        }

        if self.in_flight.is_none()
            && self.message_carried_over.is_none()
            && self.pending_ready_to_flush()
        {
            self.flush_pending();
            // Best-effort drain: the handle is unlikely to be done this
            // tick, but if it is (or there's no handle), we hand the
            // result off immediately.
            self.try_drain_in_flight(DrainMode::NonBlocking)?;
        }

        // Sample the writer's resident-memory footprint after the tick
        // has settled — pending and in_flight are mutually exclusive,
        // so this is the total compressed bytes the strategy is holding
        // (the uncompressed source bytes are already gone). Lets us
        // correlate consumer RSS with batch sizing if we hit memory
        // pressure again.
        let resident = match (&self.pending, &self.in_flight) {
            (Some(p), _) => p.compressed_bytes,
            (None, Some(b)) => b.compressed_bytes,
            (None, None) => 0,
        };
        gauge!(
            "insertions.streaming_writer.resident_compressed_bytes",
            resident as u64
        );

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<RowData>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<RowData>>> {
        if self.in_flight.is_some() || self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let commitables: Vec<(Partition, u64)> = message.committable().collect();
        let batch_size_inc = (self.compute_batch_size)(message.payload());
        let payload = message.into_payload();
        let (row_data, msg_meta) = payload.take();
        let RowData {
            encoded_rows,
            num_rows,
        } = row_data;
        let row_bytes_len = encoded_rows.len();
        let skip_write = self.skip_write;
        let pending = self.ensure_pending();

        pending.batch_size += batch_size_inc;
        pending.num_rows += num_rows;
        pending.num_bytes += row_bytes_len;
        for (partition, offset) in commitables {
            pending.offsets.insert(partition, offset);
        }
        let prev_meta = std::mem::take(&mut pending.meta);
        pending.meta = prev_meta.merge_meta(msg_meta);

        if !encoded_rows.is_empty() && !skip_write {
            // Compress in place. Any complete 1 MiB blocks are appended
            // to `chunks` so they're already in the body buffer by the
            // time we flush.
            let new_chunks = pending.compressor.push(&encoded_rows);
            if !new_chunks.is_empty() {
                let mut buf = pending.chunks.lock();
                for chunk in new_chunks {
                    pending.compressed_bytes += chunk.len();
                    buf.push(chunk);
                }
            }
        }
        // `encoded_rows` is dropped here at scope end — the source bytes
        // never outlive this `submit` call, which is the whole point of
        // the streaming path.

        Ok(())
    }

    fn terminate(&mut self) {
        self.pending = None;
        if let Some(in_flight) = self.in_flight.take() {
            if let Some(handle) = in_flight.handle {
                handle.abort();
            }
        }
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(Deadline::new);

        // Force-flush any in-progress batch so the request closes and
        // begins finishing. After this, `in_flight` is set (or there
        // was nothing pending), and we just need to wait for the HTTP
        // response and drain into next_step.
        if self.in_flight.is_none() && self.message_carried_over.is_none() {
            self.flush_pending();
        }

        // Drain until we either finish everything in flight or hit the
        // deadline. Pass the remaining deadline into the drain so
        // block_on inside it is bounded — otherwise a hung HTTP
        // response would keep `join` running indefinitely regardless
        // of the timeout we were given.
        while self.in_flight.is_some() || self.message_carried_over.is_some() {
            if deadline.is_some_and(|d| d.has_elapsed()) {
                tracing::warn!(
                    "Timeout {:?} reached while waiting for streaming writer to finish",
                    timeout
                );
                break;
            }

            let commit = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit);

            self.try_resubmit_carried_over()?;
            if self.message_carried_over.is_none() {
                let mode = match deadline {
                    Some(d) => DrainMode::BlockUpTo(d.remaining()),
                    None => DrainMode::BlockForever,
                };
                self.try_drain_in_flight(mode)?;
            }
        }

        let next_commit = self.next_step.join(deadline.map(|d| d.remaining()))?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use sentry_arroyo::types::{BrokerMessage, InnerMessage, Topic};

    use crate::config::ClickhouseConfig;
    use crate::strategies::clickhouse::writer_v2::InsertFormat;

    use super::*;

    /// Records every batch the writer hands downstream.
    struct RecordingStep {
        batches: Arc<Mutex<Vec<BytesInsertBatch<()>>>>,
    }

    impl ProcessingStrategy<BytesInsertBatch<()>> for RecordingStep {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
        fn submit(
            &mut self,
            message: Message<BytesInsertBatch<()>>,
        ) -> Result<(), SubmitError<BytesInsertBatch<()>>> {
            self.batches.lock().push(message.into_payload());
            Ok(())
        }
        fn terminate(&mut self) {}
        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    fn unreachable_config() -> ClickhouseConfig {
        // Point at a port nothing is listening on. Combined with
        // `skip_write=true` we never even reach the network.
        ClickhouseConfig {
            host: "127.0.0.1".to_string(),
            port: 9000,
            secure: false,
            http_port: 9999,
            user: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
        }
    }

    fn skip_write_client() -> Arc<ClickhouseClient> {
        Arc::new(ClickhouseClient::new(
            &unreachable_config(),
            "test_table",
            "test_storage".to_string(),
            InsertFormat::RowBinary,
            Some(&["col"]),
        ))
    }

    fn make_message(
        payload: BytesInsertBatch<RowData>,
        partition: Partition,
        offset: u64,
    ) -> Message<BytesInsertBatch<RowData>> {
        Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                payload,
                partition,
                offset,
                chrono::Utc::now(),
            )),
        }
    }

    fn batch_with(rows: usize, bytes: usize) -> BytesInsertBatch<RowData> {
        BytesInsertBatch::<RowData>::from_rows(RowData {
            encoded_rows: vec![0u8; bytes],
            num_rows: rows,
        })
        .with_num_bytes(bytes)
    }

    /// With `skip_write=true` and a row-based size limit of 2, three
    /// messages produce one full flush + one trailing flush on join.
    /// The downstream metadata batches must carry the merged offsets
    /// and num_bytes.
    #[tokio::test]
    async fn row_based_batching_skip_write() {
        crate::testutils::initialize_python();
        let runtime = Handle::current();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: recorded.clone(),
        };
        let mut strategy = StreamingClickhouseWriter::new(
            next_step,
            skip_write_client(),
            true, // skip_write
            runtime,
            2,                         // max_batch_size
            Duration::from_secs(3600), // huge time budget — only size triggers flush
            |b| b.len(),
        );

        let partition = Partition::new(Topic::new("t"), 0);

        for i in 0..3 {
            strategy
                .submit(make_message(batch_with(1, 100), partition, i))
                .expect("submit should be accepted");
            let _ = strategy.poll();
        }
        strategy
            .join(Some(Duration::from_secs(5)))
            .expect("join should not error");

        let batches = recorded.lock();
        assert_eq!(batches.len(), 2, "size-flush + join-flush");
        assert_eq!(batches[0].num_bytes(), 200, "two messages of 100B");
        assert_eq!(batches[1].num_bytes(), 100, "trailing single message");
    }

    /// Submits are rejected when an in-flight batch is queued waiting
    /// for the downstream `next_step.submit` to succeed.
    #[tokio::test]
    async fn submit_rejected_while_in_flight() {
        crate::testutils::initialize_python();
        let runtime = Handle::current();

        // Downstream step that backpressures on every submit so the
        // in-flight batch stays carried over.
        struct AlwaysReject;
        impl ProcessingStrategy<BytesInsertBatch<()>> for AlwaysReject {
            fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
            fn submit(
                &mut self,
                message: Message<BytesInsertBatch<()>>,
            ) -> Result<(), SubmitError<BytesInsertBatch<()>>> {
                Err(SubmitError::MessageRejected(MessageRejected { message }))
            }
            fn terminate(&mut self) {}
            fn join(
                &mut self,
                _: Option<Duration>,
            ) -> Result<Option<CommitRequest>, StrategyError> {
                Ok(None)
            }
        }

        let mut strategy = StreamingClickhouseWriter::new(
            AlwaysReject,
            skip_write_client(),
            true,
            runtime,
            1,
            Duration::from_secs(3600),
            |b| b.len(),
        );

        let partition = Partition::new(Topic::new("t"), 0);

        // First submit fits and triggers a flush at size=1.
        strategy
            .submit(make_message(batch_with(1, 50), partition, 0))
            .expect("first submit should be accepted");
        let _ = strategy.poll();

        // The flushed batch is carried over because next_step rejected.
        // A second submit must now be rejected.
        let res = strategy.submit(make_message(batch_with(1, 50), partition, 1));
        assert!(matches!(res, Err(SubmitError::MessageRejected(_))));
    }

    /// If the HTTP task is hung (or just very slow), `join` must
    /// return within the requested timeout instead of blocking
    /// indefinitely on the spawned future. Inject a never-resolving
    /// handle directly into `in_flight` and confirm `join(200ms)`
    /// honors the bound.
    ///
    /// Uses `#[test]` (not `#[tokio::test]`) because the strategy's
    /// `runtime.block_on(...)` panics if invoked from within an
    /// existing tokio runtime — which is fine in production (arroyo
    /// drives strategies from the consumer thread, not a tokio task)
    /// but makes `#[tokio::test]` unsuitable for any test that
    /// actually exercises the block_on path.
    #[test]
    fn join_respects_timeout_with_hung_http() {
        crate::testutils::initialize_python();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build tokio runtime");
        let handle = runtime.handle().clone();

        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: recorded.clone(),
        };
        let mut strategy = StreamingClickhouseWriter::new(
            next_step,
            skip_write_client(),
            true,
            handle.clone(),
            10,
            Duration::from_secs(3600),
            |b| b.len(),
        );

        // Spawn a task on the runtime that pretends to be the HTTP
        // request but never resolves. Without the timeout-bounded
        // drain, `join` would wait on it forever via `block_on(handle)`.
        let stuck = handle.spawn(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            anyhow::bail!("would have resolved if we ever got here")
        });
        strategy.in_flight = Some(InFlightBatch {
            handle: Some(stuck),
            num_rows: 0,
            num_bytes: 0,
            compressed_bytes: 0,
            offsets: BTreeMap::new(),
            meta: BytesInsertBatch::<()>::default(),
            write_start: SystemTime::now(),
        });

        let join_timeout = Duration::from_millis(200);
        let started = std::time::Instant::now();
        strategy
            .join(Some(join_timeout))
            .expect("join should not error on timeout");
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "join took {elapsed:?}; should have honored the {join_timeout:?} budget"
        );
        // The stuck batch was aborted, not flushed downstream — the
        // recording step must not have seen anything.
        assert!(recorded.lock().is_empty());
    }

    /// `join` on an empty strategy is a no-op — never opens a request,
    /// never hangs.
    #[tokio::test]
    async fn join_with_no_messages_returns_immediately() {
        crate::testutils::initialize_python();
        let runtime = Handle::current();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: recorded.clone(),
        };
        let mut strategy = StreamingClickhouseWriter::new(
            next_step,
            skip_write_client(),
            true,
            runtime,
            10,
            Duration::from_secs(3600),
            |b| b.len(),
        );

        strategy
            .join(Some(Duration::from_secs(1)))
            .expect("join should not error");
        assert!(recorded.lock().is_empty());
    }
}
