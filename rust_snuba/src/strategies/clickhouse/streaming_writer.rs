//! Streaming write path for ClickHouse INSERTs.
//!
//! Replaces the `Reduce → ClickhouseWriterStep` pair for the RowBinary
//! path. Instead of accumulating the full uncompressed batch in a
//! `BytesInsertBatch<RowData>` and then handing it to a buffered writer
//! that compresses and sends in one shot, this strategy compresses rows
//! into ClickHouse-native LZ4 blocks as messages arrive and pushes the
//! resulting `Bytes` onto an HTTP body channel feeding a single in-flight
//! POST per batch. The encoded uncompressed bytes are dropped per
//! message; the compressed chunks live in the `retry_buf` (and in the
//! mpsc until reqwest drains them).
//!
//! Concurrency is fixed at one in-flight batch — submits are rejected
//! while we're awaiting the previous batch's response. The existing
//! buffered path uses `RunTaskInThreads` for inter-batch concurrency;
//! we sacrifice that here for a much smaller resident set and a simpler
//! state machine.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::Response;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use sentry_arroyo::utils::timing::Deadline;
use sentry_arroyo::{counter, timer};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::streaming_lz4::StreamingLz4Compressor;
use super::writer_v2::{ClickhouseClient, RetryConfig};
use crate::types::{BytesInsertBatch, RowData};

/// State for the batch currently being accumulated. Holds the running
/// metadata plus, once the first non-empty message arrives, the
/// streaming compression + HTTP request handle.
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
    offsets: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    /// Wall-clock at the moment the first message of the batch arrived.
    /// Used to compute `insertions.batch_write_ms` on completion.
    write_start: SystemTime,
    /// `None` until the first non-empty, non-skipped message lands. We
    /// don't open an HTTP request for empty batches (commit-only flushes).
    streaming: Option<StreamingState>,
}

struct StreamingState {
    compressor: StreamingLz4Compressor,
    /// Producer side of the request body channel. Dropping it signals
    /// end-of-body to reqwest, which then finalizes the POST.
    body_tx: mpsc::UnboundedSender<Result<Bytes, io::Error>>,
    /// Shared with the spawned `send_streamed` task — every compressed
    /// chunk we push to `body_tx` is also recorded here so retries can
    /// replay the body without re-running the (already-consumed) stream.
    retry_buf: Arc<std::sync::Mutex<Vec<Bytes>>>,
    handle: JoinHandle<anyhow::Result<Response>>,
}

/// A batch whose body has been finalized and is awaiting the HTTP
/// response (or, for batches that never opened an HTTP request, ready
/// to be submitted downstream immediately).
struct InFlightBatch {
    handle: Option<JoinHandle<anyhow::Result<Response>>>,
    num_rows: usize,
    num_bytes: usize,
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

    /// Ensure `self.pending` exists with the streaming state initialized
    /// up to (but not including) the first compressed chunk. Spawns the
    /// `send_streamed` task lazily — the first message of a batch may
    /// be empty (or `skip_write` may be set), in which case we never open
    /// an HTTP request and the in-flight batch carries `handle = None`.
    fn ensure_pending(&mut self) -> &mut PendingBatch {
        self.pending.get_or_insert_with(|| PendingBatch {
            batch_start: Deadline::new(self.max_batch_time),
            batch_size: 0,
            num_rows: 0,
            num_bytes: 0,
            offsets: BTreeMap::new(),
            meta: BytesInsertBatch::<()>::default(),
            write_start: SystemTime::now(),
            streaming: None,
        })
    }

    /// Lazy-start the HTTP request task on the first non-empty message.
    fn ensure_streaming(
        client: Arc<ClickhouseClient>,
        runtime: &Handle,
        pending: &mut PendingBatch,
    ) {
        if pending.streaming.is_some() {
            return;
        }
        let (body_tx, body_rx) = mpsc::unbounded_channel::<Result<Bytes, io::Error>>();
        let retry_buf: Arc<std::sync::Mutex<Vec<Bytes>>> = Arc::new(Default::default());
        let stream = UnboundedReceiverStream::new(body_rx);
        let retry_buf_for_task = retry_buf.clone();
        let handle = runtime.spawn(async move {
            client
                .send_streamed(stream, retry_buf_for_task, RetryConfig::default())
                .await
        });
        pending.streaming = Some(StreamingState {
            compressor: StreamingLz4Compressor::new(),
            body_tx,
            retry_buf,
            handle,
        });
    }

    /// Append `bytes` to the active streaming compressor. Every chunk
    /// that pops out of the compressor is teed into `retry_buf` (so
    /// retries can replay it) and pushed onto `body_tx`. Send errors on
    /// `body_tx` are intentionally ignored: they signal that the first
    /// HTTP attempt consumed-and-dropped the stream after a network
    /// error, and `send_streamed`'s retry loop will pick up from
    /// `retry_buf` once we close the body.
    fn push_bytes(streaming: &mut StreamingState, bytes: &[u8]) {
        let chunks = streaming.compressor.push(bytes);
        if chunks.is_empty() {
            return;
        }
        {
            let mut buf = streaming.retry_buf.lock().unwrap();
            for chunk in &chunks {
                buf.push(chunk.clone());
            }
        }
        for chunk in chunks {
            let _ = streaming.body_tx.send(Ok(chunk));
        }
    }

    /// Move `pending` → `in_flight`: drain the compressor's tail (if
    /// any), drop the sender to close the HTTP body, and stash the
    /// `JoinHandle` for `poll` to await. Must be called only when
    /// `in_flight` is `None` (the strategy keeps at most one batch in
    /// flight at a time).
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
            offsets,
            meta,
            write_start,
            streaming,
        } = pending;
        let handle = streaming.map(|s| {
            let StreamingState {
                compressor,
                body_tx,
                retry_buf,
                handle,
            } = s;
            if let Some(last) = compressor.finish() {
                retry_buf.lock().unwrap().push(last.clone());
                let _ = body_tx.send(Ok(last));
            }
            // Dropping `body_tx` here closes the HTTP body — `send_streamed`
            // sees end-of-stream and finalizes the POST.
            drop(body_tx);
            handle
        });
        self.in_flight = Some(InFlightBatch {
            handle,
            num_rows,
            num_bytes,
            offsets,
            meta,
            write_start,
        });
    }

    /// Try to drain the in-flight batch. If `blocking` is false, returns
    /// without doing work when the HTTP request hasn't finished yet. If
    /// `blocking`, waits for the request to complete (used by `join`).
    ///
    /// On success: emits per-batch metrics, builds an any-message with
    /// the merged meta + offsets, and submits it to `next_step`. A
    /// downstream `MessageRejected` is stashed in `message_carried_over`
    /// for `poll` to retry — we still consider `in_flight` drained.
    fn try_drain_in_flight(&mut self, blocking: bool) -> Result<(), StrategyError> {
        let Some(mut in_flight) = self.in_flight.take() else {
            return Ok(());
        };
        match in_flight.handle.take() {
            None => {}
            Some(handle) => {
                if !blocking && !handle.is_finished() {
                    in_flight.handle = Some(handle);
                    self.in_flight = Some(in_flight);
                    return Ok(());
                }
                match self.runtime.block_on(handle) {
                    Ok(Ok(_response)) => {}
                    Ok(Err(e)) => return Err(StrategyError::Other(e.into())),
                    Err(e) => return Err(StrategyError::Other(Box::new(e))),
                }
            }
        }

        let write_finish = SystemTime::now();
        if let Ok(elapsed) = write_finish.duration_since(in_flight.write_start) {
            timer!("insertions.batch_write_ms", elapsed);
        }
        counter!("insertions.batch_write_bytes", in_flight.num_bytes as i64);
        counter!("insertions.batch_write_msgs", in_flight.num_rows as i64);
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
            self.try_drain_in_flight(false)?;
        }

        if self.in_flight.is_none()
            && self.message_carried_over.is_none()
            && self.pending_ready_to_flush()
        {
            self.flush_pending();
            // Best-effort drain: the handle is unlikely to be done this
            // tick, but if it is (or there's no handle), we hand the
            // result off immediately.
            self.try_drain_in_flight(false)?;
        }

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

        let client = self.client.clone();
        let runtime = self.runtime.clone();
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
            Self::ensure_streaming(client, &runtime, pending);
            // safety: `ensure_streaming` populates `streaming`.
            let streaming = pending.streaming.as_mut().unwrap();
            Self::push_bytes(streaming, &encoded_rows);
        }
        // Free the encoded uncompressed bytes immediately. (Already
        // dropped on the previous line — the explicit drop here is
        // defensive against a future edit that adds a branch which
        // forgets to consume `encoded_rows`.)
        drop(encoded_rows);

        Ok(())
    }

    fn terminate(&mut self) {
        if let Some(pending) = self.pending.take() {
            if let Some(streaming) = pending.streaming {
                streaming.handle.abort();
            }
        }
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
        // deadline. `try_drain_in_flight(true)` blocks on the handle, so
        // this loop won't spin.
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
                self.try_drain_in_flight(true)?;
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
    use std::sync::Mutex;

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
            self.batches.lock().unwrap().push(message.into_payload());
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
                .unwrap();
            let _ = strategy.poll();
        }
        let _ = strategy.join(Some(Duration::from_secs(5))).unwrap();

        let batches = recorded.lock().unwrap();
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
            .unwrap();
        let _ = strategy.poll();

        // The flushed batch is carried over because next_step rejected.
        // A second submit must now be rejected.
        let res = strategy.submit(make_message(batch_with(1, 50), partition, 1));
        assert!(matches!(res, Err(SubmitError::MessageRejected(_))));
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

        strategy.join(Some(Duration::from_secs(1))).unwrap();
        assert!(recorded.lock().unwrap().is_empty());
    }
}
