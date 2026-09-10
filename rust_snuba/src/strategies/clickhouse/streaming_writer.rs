//! Streaming write path for ClickHouse INSERTs.
//!
//! Replaces the `Reduce → RowBinaryWriterStep` pair for the RowBinary
//! path. Encoded rows are LZ4-compressed as they arrive and pushed onto
//! an in-flight HTTP POST; the uncompressed bytes are dropped per
//! message, so we never hold a full uncompressed batch.
//!
//! The POST starts on the first non-empty payload of a batch (live
//! streaming) rather than at flush. Closing the body channel is what
//! ClickHouse treats as end-of-INSERT, so we only drop the sender in
//! `flush_pending`. Abandoning a batch aborts the HTTP task first so a
//! connection reset cannot land as a successful partial write.
//!
//! If the live attempt fails mid-batch, retries wait for
//! `body_complete` so they replay the full compressed body, not a
//! prefix. That was the truncation bug that forced #8001 to POST at
//! flush time.
//!
//! Inter-batch concurrency is `max_in_flight` (the same
//! `clickhouse_concurrency` the buffered writer uses). #8001 capped this
//! at one and the resulting submit backpressure showed up as consumer
//! pause time; we keep N slots and only reject once they are all busy.
//! In-flight batches complete in order so a later offset cannot commit
//! before an earlier write.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

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
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::streaming_lz4::StreamingLz4Compressor;
use super::writer_v2::ClickhouseClient;
use crate::types::{BytesInsertBatch, RowData};

struct PendingBatch {
    batch_start: Deadline,
    batch_size: usize,
    num_rows: usize,
    num_bytes: usize,
    compressed_bytes: usize,
    offsets: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    write_start: Instant,
    compressor: StreamingLz4Compressor,
    chunks: std::sync::Arc<Mutex<Vec<Bytes>>>,
    tx: Option<UnboundedSender<Bytes>>,
    complete_tx: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<Response>>>,
}

impl PendingBatch {
    fn push_chunks(&mut self, chunks: Vec<Bytes>) {
        if chunks.is_empty() {
            return;
        }
        let mut buf = self.chunks.lock();
        for chunk in chunks {
            self.compressed_bytes += chunk.len();
            if let Some(tx) = &self.tx {
                let _ = tx.send(chunk.clone());
            }
            buf.push(chunk);
        }
    }
}

enum DrainMode {
    NonBlocking,
    BlockForever,
    BlockUpTo(Duration),
}

struct InFlightBatch {
    handle: Option<JoinHandle<anyhow::Result<Response>>>,
    num_rows: usize,
    num_bytes: usize,
    compressed_bytes: usize,
    offsets: BTreeMap<Partition, u64>,
    meta: BytesInsertBatch<()>,
    write_start: Instant,
}

pub struct StreamingClickhouseWriter<N> {
    next_step: N,
    client: std::sync::Arc<ClickhouseClient>,
    skip_write: bool,
    runtime: Handle,
    max_batch_size: usize,
    max_batch_time: Duration,
    max_in_flight: usize,
    compute_batch_size: fn(&BytesInsertBatch<RowData>) -> usize,

    pending: Option<PendingBatch>,
    in_flight: VecDeque<InFlightBatch>,

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
        client: std::sync::Arc<ClickhouseClient>,
        skip_write: bool,
        runtime: Handle,
        max_in_flight: usize,
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
            max_in_flight: max_in_flight.max(1),
            compute_batch_size,
            pending: None,
            in_flight: VecDeque::new(),
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
            write_start: Instant::now(),
            compressor: StreamingLz4Compressor::new(),
            chunks: std::sync::Arc::new(Mutex::new(Vec::new())),
            tx: None,
            complete_tx: None,
            handle: None,
        })
    }

    /// Open the live HTTP POST for `pending`. No-op if we already have a
    /// sender, we're skipping writes, or there is nothing to send.
    fn start_stream(&mut self) {
        if self.skip_write {
            return;
        }
        let retry_buf = match &self.pending {
            Some(pending) if pending.tx.is_none() && pending.handle.is_none() => {
                pending.chunks.clone()
            }
            _ => return,
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (complete_tx, complete_rx) = tokio::sync::oneshot::channel();
        let client = self.client.clone();
        let extra = self.max_batch_time;
        let handle = self.runtime.spawn(async move {
            let stream = futures::stream::unfold(rx, |mut rx| async move {
                rx.recv()
                    .await
                    .map(|b| (Ok::<Bytes, std::io::Error>(b), rx))
            });
            client
                .send_streamed(stream, retry_buf, complete_rx, extra)
                .await
        });

        let pending = self.pending.as_mut().expect("checked above");
        pending.tx = Some(tx);
        pending.complete_tx = Some(complete_tx);
        pending.handle = Some(handle);
    }

    /// Finish the compressor, close the live body (clean EOF), signal
    /// `body_complete` so retries may fire, and move the batch onto the
    /// in-flight queue.
    fn flush_pending(&mut self) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let PendingBatch {
            compressor,
            chunks,
            tx,
            complete_tx,
            handle,
            mut compressed_bytes,
            num_rows,
            num_bytes,
            offsets,
            meta,
            write_start,
            ..
        } = pending;

        if let Some(last) = compressor.finish() {
            compressed_bytes += last.len();
            if let Some(ref tx) = tx {
                let _ = tx.send(last.clone());
            }
            chunks.lock().push(last);
        }

        // Close the live body only here. Dropping `tx` is end-of-INSERT
        // for ClickHouse; doing it earlier would ACK a truncated batch.
        drop(tx);
        if let Some(complete_tx) = complete_tx {
            let _ = complete_tx.send(());
        }

        self.in_flight.push_back(InFlightBatch {
            handle,
            num_rows,
            num_bytes,
            compressed_bytes,
            offsets,
            meta,
            write_start,
        });
    }

    /// Abort the HTTP task *before* dropping the body sender, so the
    /// connection resets instead of completing a partial INSERT.
    fn abandon_pending(&mut self) {
        let Some(mut pending) = self.pending.take() else {
            return;
        };
        if let Some(handle) = pending.handle.take() {
            handle.abort();
        }
        tracing::warn!(
            "Abandoning pending streaming batch ({} rows, {} uncompressed bytes)",
            pending.num_rows,
            pending.num_bytes
        );
        // `tx` / `complete_tx` drop after the abort: the request is
        // already cancelled, so EOF cannot land as a successful write.
    }

    fn try_drain_front(&mut self, mode: DrainMode) -> Result<(), StrategyError> {
        let Some(mut in_flight) = self.in_flight.pop_front() else {
            return Ok(());
        };
        match in_flight.handle.take() {
            None => {}
            Some(mut handle) => match mode {
                DrainMode::NonBlocking => {
                    if !handle.is_finished() {
                        in_flight.handle = Some(handle);
                        self.in_flight.push_front(in_flight);
                        return Ok(());
                    }
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
                                "Streaming HTTP write exceeded {:?}; aborted in-flight task. \
                                 Batch metadata not committed downstream — the next consumer \
                                 instance will retry from the last committed offset.",
                                max_wait
                            );
                            return Ok(());
                        }
                    }
                }
            },
        }

        timer!("insertions.batch_write_ms", in_flight.write_start.elapsed());
        counter!("insertions.batch_write_bytes", in_flight.num_bytes as i64);
        counter!("insertions.batch_write_msgs", in_flight.num_rows as i64);
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

    fn resident_compressed_bytes(&self) -> usize {
        let pending = self.pending.as_ref().map_or(0, |p| p.compressed_bytes);
        let in_flight = self
            .in_flight
            .iter()
            .map(|b| b.compressed_bytes)
            .sum::<usize>();
        pending + in_flight
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

        while self.message_carried_over.is_none() && !self.in_flight.is_empty() {
            let before = self.in_flight.len();
            self.try_drain_front(DrainMode::NonBlocking)?;
            if self.in_flight.len() == before {
                break;
            }
        }

        if self.message_carried_over.is_none() && self.pending_ready_to_flush() {
            self.flush_pending();
            self.try_drain_front(DrainMode::NonBlocking)?;
        }

        gauge!(
            "insertions.streaming_writer.resident_compressed_bytes",
            self.resident_compressed_bytes() as u64
        );
        gauge!(
            "insertions.streaming_writer.in_flight",
            self.in_flight.len() as u64
        );

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(
        &mut self,
        message: Message<BytesInsertBatch<RowData>>,
    ) -> Result<(), SubmitError<BytesInsertBatch<RowData>>> {
        if self.message_carried_over.is_some()
            || (self.pending.is_none() && self.in_flight.len() >= self.max_in_flight)
        {
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

        self.ensure_pending();
        if !encoded_rows.is_empty() && !skip_write {
            self.start_stream();
        }

        let pending = self.pending.as_mut().expect("ensure_pending just ran");
        pending.batch_size += batch_size_inc;
        pending.num_rows += num_rows;
        pending.num_bytes += row_bytes_len;
        for (partition, offset) in commitables {
            pending.offsets.insert(partition, offset);
        }
        let prev_meta = std::mem::take(&mut pending.meta);
        pending.meta = prev_meta.merge_meta(msg_meta);

        if !encoded_rows.is_empty() && !skip_write {
            let new_chunks = pending.compressor.push(&encoded_rows);
            pending.push_chunks(new_chunks);
        }

        Ok(())
    }

    fn terminate(&mut self) {
        self.abandon_pending();
        while let Some(in_flight) = self.in_flight.pop_front() {
            if let Some(handle) = in_flight.handle {
                handle.abort();
            }
        }
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(Deadline::new);

        if deadline.is_some_and(|d| d.has_elapsed()) {
            self.abandon_pending();
        } else {
            self.flush_pending();
        }

        while !self.in_flight.is_empty() || self.message_carried_over.is_some() {
            if deadline.is_some_and(|d| d.has_elapsed()) {
                while let Some(in_flight) = self.in_flight.pop_front() {
                    if let Some(handle) = in_flight.handle {
                        handle.abort();
                    }
                    tracing::warn!(
                        "Timeout {:?} reached during streaming-writer join; aborted in-flight batch ({} rows)",
                        timeout,
                        in_flight.num_rows
                    );
                }
                self.message_carried_over = None;
                break;
            }

            let commit = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit);

            self.try_resubmit_carried_over()?;
            if self.message_carried_over.is_none() && !self.in_flight.is_empty() {
                let mode = match deadline {
                    Some(d) => DrainMode::BlockUpTo(d.remaining()),
                    None => DrainMode::BlockForever,
                };
                self.try_drain_front(mode)?;
            } else if self.message_carried_over.is_some() {
                std::thread::sleep(Duration::from_millis(10));
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
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;
    use sentry_arroyo::processing::strategies::{
        CommitRequest, MessageRejected, ProcessingStrategy, StrategyError, SubmitError,
    };
    use sentry_arroyo::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use tokio::runtime::Handle;

    use crate::config::ClickhouseConfig;
    use crate::strategies::clickhouse::writer_v2::{ClickhouseClient, InsertFormat};
    use crate::types::{BytesInsertBatch, RowData};

    use super::*;

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

    fn unreachable_client() -> std::sync::Arc<ClickhouseClient> {
        std::sync::Arc::new(ClickhouseClient::new(
            &ClickhouseConfig {
                host: "127.0.0.1".to_string(),
                port: 1,
                secure: false,
                user: "default".to_string(),
                password: "".to_string(),
                database: "default".to_string(),
            },
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

    fn hung_in_flight(handle: Handle) -> InFlightBatch {
        let stuck = handle.spawn(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            anyhow::bail!("would have resolved if we ever got here")
        });
        InFlightBatch {
            handle: Some(stuck),
            num_rows: 0,
            num_bytes: 0,
            compressed_bytes: 0,
            offsets: BTreeMap::new(),
            meta: BytesInsertBatch::<()>::default(),
            write_start: Instant::now(),
        }
    }

    /// With `skip_write=true` and a row-based size limit of 2, three
    /// messages produce one full flush + one trailing flush on join.
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
            unreachable_client(),
            true,
            runtime,
            2,
            2,
            Duration::from_secs(3600),
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

    #[tokio::test]
    async fn submit_rejected_while_downstream_backpressures() {
        crate::testutils::initialize_python();
        let runtime = Handle::current();

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
            unreachable_client(),
            true,
            runtime,
            2,
            1,
            Duration::from_secs(3600),
            |b| b.len(),
        );

        let partition = Partition::new(Topic::new("t"), 0);
        strategy
            .submit(make_message(batch_with(1, 50), partition, 0))
            .expect("first submit should be accepted");
        let _ = strategy.poll();

        let res = strategy.submit(make_message(batch_with(1, 50), partition, 1));
        assert!(matches!(res, Err(SubmitError::MessageRejected(_))));
    }

    #[test]
    fn submit_accepted_when_a_slot_is_free() {
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
            unreachable_client(),
            true,
            handle.clone(),
            2,
            10,
            Duration::from_secs(3600),
            |b| b.len(),
        );
        strategy.in_flight.push_back(hung_in_flight(handle));

        let partition = Partition::new(Topic::new("t"), 0);
        strategy
            .submit(make_message(batch_with(1, 50), partition, 0))
            .expect("one busy slot of two must not reject submits");
    }

    #[test]
    fn submit_rejected_when_all_slots_full() {
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
            unreachable_client(),
            true,
            handle.clone(),
            2,
            10,
            Duration::from_secs(3600),
            |b| b.len(),
        );
        strategy.in_flight.push_back(hung_in_flight(handle.clone()));
        strategy.in_flight.push_back(hung_in_flight(handle));

        let partition = Partition::new(Topic::new("t"), 0);
        let res = strategy.submit(make_message(batch_with(1, 50), partition, 0));
        assert!(matches!(res, Err(SubmitError::MessageRejected(_))));
    }

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
            unreachable_client(),
            true,
            handle.clone(),
            2,
            10,
            Duration::from_secs(3600),
            |b| b.len(),
        );
        strategy.in_flight.push_back(hung_in_flight(handle));

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
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn join_with_elapsed_deadline_abandons_pending() {
        crate::testutils::initialize_python();
        let runtime = Handle::current();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let next_step = RecordingStep {
            batches: recorded.clone(),
        };
        let mut strategy = StreamingClickhouseWriter::new(
            next_step,
            unreachable_client(),
            false,
            runtime,
            2,
            10,
            Duration::from_secs(3600),
            |b| b.len(),
        );

        let partition = Partition::new(Topic::new("t"), 0);
        strategy
            .submit(make_message(batch_with(1, 50), partition, 0))
            .expect("submit should be accepted");

        strategy
            .join(Some(Duration::ZERO))
            .expect("join should not error");

        assert!(
            recorded.lock().is_empty(),
            "elapsed-deadline join must not propagate the pending batch downstream"
        );
        assert!(strategy.pending.is_none());
        assert!(strategy.in_flight.is_empty());
    }

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
            unreachable_client(),
            true,
            runtime,
            2,
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
