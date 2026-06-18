//! # Stuck Partition Watchdog
//!
//! Detects when a *single* partition stops making progress for a sustained
//! period of time while the rest of the consumer keeps consuming, and reacts by
//! disconnecting from and reconnecting to the broker.
//!
//! ## Why this exists
//!
//! `librdkafka` occasionally gets into a state where it stops fetching from a
//! single partition (a "stuck"/"stalled" partition) while continuing to consume
//! from the other partitions assigned to the same consumer. The consumer keeps
//! making progress overall, so it looks healthy, but the lag on the stuck
//! partition grows without bound. The well-known remedy is to force the client
//! to re-establish its connection to the broker.
//!
//! ## How detection works
//!
//! A stuck partition delivers **no** messages, so we cannot observe it directly
//! from the message stream. Instead we track, per partition, the wall-clock time
//! at which we last received a message (updated on every [`submit`]). On every
//! [`poll`] we look for a partition that has been silent for at least
//! `timeout` (default 5 minutes) **while at least one other partition has
//! produced a message within that same window**.
//!
//! Requiring another partition to still be active is what lets us distinguish a
//! genuinely stuck partition from:
//!   * a globally stalled consumer (broker outage, backpressure, paused
//!     consumer) — in that case *all* partitions go silent together, so no
//!     single partition stands out, and
//!   * an idle topic/partition that simply has no new data.
//!
//! Because of the "some other partition must be active" guard, the watchdog only
//! ever fires when a single partition lags behind its peers, which is exactly the
//! condition we want to recover from. It is intended for high-throughput
//! consumers whose partitions receive data continuously; for low-volume topics a
//! legitimately idle partition could be misread as stuck, which is why the
//! feature is gated behind a runtime flag (see [`StuckPartitionWatchdog::enabled`]).
//!
//! ## How recovery works
//!
//! Arroyo does not expose a way to surgically reconnect a single partition (or
//! even the whole consumer) from within a processing strategy — the only
//! consumer-control lever available to Snuba is
//! [`ProcessorHandle::signal_shutdown`]. When the watchdog fires it triggers a
//! graceful shutdown of the [`StreamProcessor`]. The Rust consumer then returns,
//! the Python entrypoint calls `sys.exit`, and the supervising process manager
//! (e.g. Kubernetes) restarts the consumer, which establishes a brand new
//! connection to the broker. In other words: "disconnect and reconnect to the
//! broker" is implemented as a clean shutdown followed by a supervised restart.
//!
//! [`submit`]: ProcessingStrategy::submit
//! [`poll`]: ProcessingStrategy::poll
//! [`StreamProcessor`]: sentry_arroyo::processing::StreamProcessor
//! [`ProcessorHandle::signal_shutdown`]: sentry_arroyo::processing::ProcessorHandle::signal_shutdown

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::processing::ProcessorHandle;
use sentry_arroyo::types::{Message, Partition};

use crate::runtime_config::get_str_config;

/// Default time a partition may stay silent before it is considered stuck.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// How often we evaluate the partitions for stuckness. Detection does not need
/// to be precise to the second, and this keeps the (Python-backed) runtime
/// config lookups off the hot path.
const CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// A shared, thread-safe handle that lets a processing strategy ask the
/// [`StreamProcessor`] to shut down so the consumer is restarted and reconnects
/// to the broker.
///
/// The [`ProcessorHandle`] only exists *after* the `StreamProcessor` has been
/// constructed, whereas the strategy factory (which ultimately owns the
/// watchdog) is built *before* it. We therefore hand the factory an empty
/// `ReconnectSignal` up front and inject the handle via [`set_handle`] once the
/// processor is available.
///
/// [`StreamProcessor`]: sentry_arroyo::processing::StreamProcessor
/// [`set_handle`]: ReconnectSignal::set_handle
#[derive(Clone, Default)]
pub struct ReconnectSignal {
    handle: Arc<Mutex<Option<ProcessorHandle>>>,
    fired: Arc<AtomicBool>,
}

impl ReconnectSignal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Provide the [`ProcessorHandle`] used to request a shutdown. Should be
    /// called exactly once, right after the `StreamProcessor` is created.
    pub fn set_handle(&self, handle: ProcessorHandle) {
        *self.handle.lock() = Some(handle);
    }

    /// Request a graceful shutdown of the consumer at most once. Returns `true`
    /// if this call is the one that initiated the shutdown, `false` if a
    /// shutdown had already been requested.
    pub fn trigger(&self) -> bool {
        if self.fired.swap(true, Ordering::SeqCst) {
            return false;
        }
        match self.handle.lock().as_mut() {
            Some(handle) => handle.signal_shutdown(),
            None => tracing::error!(
                "ReconnectSignal triggered before a ProcessorHandle was set; \
                 consumer will not be restarted"
            ),
        }
        true
    }

    /// Whether a shutdown has already been requested through this signal.
    pub fn has_fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

/// Processing strategy that watches for a single stuck partition and triggers a
/// reconnect when one is found. See the module docs for details.
pub struct StuckPartitionWatchdog<Next> {
    next_step: Next,
    signal: ReconnectSignal,
    /// Wall-clock time at which we last saw a message for each partition.
    last_seen: HashMap<Partition, Instant>,
    /// Last time we evaluated the partitions, to throttle checks to
    /// `CHECK_INTERVAL`.
    last_check: Instant,
}

impl<Next> StuckPartitionWatchdog<Next> {
    pub fn new(next_step: Next, signal: ReconnectSignal) -> Self {
        Self {
            next_step,
            signal,
            last_seen: HashMap::new(),
            last_check: Instant::now(),
        }
    }

    /// Whether the watchdog is enabled. Off by default; enable by setting the
    /// `stuck_partition_watchdog_enabled` runtime config to `"1"`.
    fn enabled() -> bool {
        get_str_config("stuck_partition_watchdog_enabled")
            .ok()
            .flatten()
            .as_deref()
            == Some("1")
    }

    /// The silence threshold after which a partition is considered stuck. Read
    /// from the `stuck_partition_watchdog_timeout_seconds` runtime config,
    /// falling back to [`DEFAULT_TIMEOUT`] when unset or invalid.
    fn timeout() -> Duration {
        get_str_config("stuck_partition_watchdog_timeout_seconds")
            .ok()
            .flatten()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_TIMEOUT)
    }

    /// Returns a partition that has been silent for at least `timeout` while at
    /// least one *other* partition produced a message within that same window.
    ///
    /// Returns `None` when no partition stands out — in particular when every
    /// partition is silent (a global stall, not a single stuck partition) or
    /// when only a single partition is assigned (we cannot tell "stuck" from
    /// "idle" without a peer to compare against).
    fn find_stuck_partition(&self, now: Instant, timeout: Duration) -> Option<Partition> {
        // Is any partition still actively delivering messages? If not, this is a
        // global stall rather than a single stuck partition, and reconnecting
        // would not help.
        let any_active = self
            .last_seen
            .values()
            .any(|seen| now.duration_since(*seen) < timeout);
        if !any_active {
            return None;
        }

        // Pick the lowest-indexed silent partition for deterministic behaviour.
        self.last_seen
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) >= timeout)
            .map(|(partition, _)| *partition)
            .min_by_key(|partition| partition.index)
    }
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for StuckPartitionWatchdog<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();

        let now = Instant::now();
        if now.duration_since(self.last_check) >= CHECK_INTERVAL {
            self.last_check = now;

            // Skip the work (including the runtime-config lookups) if we have
            // already asked the consumer to shut down.
            if !self.signal.has_fired() && Self::enabled() {
                let timeout = Self::timeout();
                if let Some(partition) = self.find_stuck_partition(now, timeout) {
                    let silent_for = self
                        .last_seen
                        .get(&partition)
                        .map(|seen| now.duration_since(*seen))
                        .unwrap_or_default();

                    tracing::error!(
                        partition = partition.index,
                        topic = %partition.topic,
                        silent_for_secs = silent_for.as_secs(),
                        timeout_secs = timeout.as_secs(),
                        "Partition has not produced a message for at least the configured timeout \
                         while other partitions are still active; triggering a consumer shutdown \
                         to reconnect to the broker",
                    );
                    counter!(
                        "stuck_partition_watchdog.reconnect_triggered",
                        1,
                        "partition" => partition.index.to_string()
                    );

                    self.signal.trigger();
                }
            }
        }

        poll_result
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        let now = Instant::now();
        for (partition, _offset) in message.committable() {
            self.last_seen.insert(partition, now);
        }
        self.next_step.submit(message)
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::patch_str_config_for_test;
    use sentry_arroyo::backends::kafka::types::KafkaPayload;
    use sentry_arroyo::types::{Partition, Topic};
    use std::time::Duration;

    /// Records what it received so tests can assert on forwarding behaviour.
    struct MockStrategy {
        submitted: usize,
        polled: usize,
        terminated: bool,
        joined: bool,
    }

    impl MockStrategy {
        fn new() -> Self {
            Self {
                submitted: 0,
                polled: 0,
                terminated: false,
                joined: false,
            }
        }
    }

    impl ProcessingStrategy<KafkaPayload> for MockStrategy {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            self.polled += 1;
            Ok(None)
        }

        fn submit(
            &mut self,
            _message: Message<KafkaPayload>,
        ) -> Result<(), SubmitError<KafkaPayload>> {
            self.submitted += 1;
            Ok(())
        }

        fn terminate(&mut self) {
            self.terminated = true;
        }

        fn join(
            &mut self,
            _timeout: Option<Duration>,
        ) -> Result<Option<CommitRequest>, StrategyError> {
            self.joined = true;
            Ok(None)
        }
    }

    fn make_message(partition_index: u16) -> Message<KafkaPayload> {
        Message::new_broker_message(
            KafkaPayload::new(None, None, Some(b"test".to_vec())),
            Partition::new(Topic::new("test"), partition_index),
            0,
            chrono::Utc::now(),
        )
    }

    /// A partition silent past the timeout, while another partition is still
    /// active, is detected as stuck.
    #[test]
    fn test_detects_single_stuck_partition() {
        let now = Instant::now();
        let watchdog = StuckPartitionWatchdog::new(MockStrategy::new(), ReconnectSignal::new());

        let p0 = Partition::new(Topic::new("test"), 0);
        let p1 = Partition::new(Topic::new("test"), 1);

        // p0 went silent two seconds ago, p1 is active right now.
        let mut watchdog = watchdog;
        watchdog.last_seen.insert(p0, now - Duration::from_secs(2));
        watchdog.last_seen.insert(p1, now);

        assert_eq!(
            watchdog.find_stuck_partition(now, Duration::from_secs(1)),
            Some(p0)
        );
    }

    /// When every partition is silent (a global stall), nothing is flagged.
    #[test]
    fn test_global_stall_not_flagged() {
        let now = Instant::now();
        let mut watchdog = StuckPartitionWatchdog::new(MockStrategy::new(), ReconnectSignal::new());

        let p0 = Partition::new(Topic::new("test"), 0);
        let p1 = Partition::new(Topic::new("test"), 1);
        watchdog.last_seen.insert(p0, now - Duration::from_secs(5));
        watchdog.last_seen.insert(p1, now - Duration::from_secs(5));

        assert_eq!(
            watchdog.find_stuck_partition(now, Duration::from_secs(1)),
            None
        );
    }

    /// A single assigned partition is never flagged, because there is no active
    /// peer to compare it against (idle vs stuck is indistinguishable).
    #[test]
    fn test_single_partition_not_flagged() {
        let now = Instant::now();
        let mut watchdog = StuckPartitionWatchdog::new(MockStrategy::new(), ReconnectSignal::new());

        let p0 = Partition::new(Topic::new("test"), 0);
        watchdog.last_seen.insert(p0, now - Duration::from_secs(60));

        assert_eq!(
            watchdog.find_stuck_partition(now, Duration::from_secs(1)),
            None
        );
    }

    /// End-to-end `poll` behaviour: it always forwards to the next step, never
    /// triggers while disabled, and once enabled fires the reconnect signal
    /// exactly once when a stuck partition is present.
    ///
    /// Disabled and enabled cases live in a single test on purpose: they share
    /// the process-global runtime-config key, so splitting them into separate
    /// tests would race under cargo's parallel test runner.
    #[test]
    fn test_poll_respects_enabled_flag_and_triggers_once() {
        let signal = ReconnectSignal::new();
        let mut watchdog = StuckPartitionWatchdog::new(MockStrategy::new(), signal.clone());

        let p0 = Partition::new(Topic::new("test"), 0);
        let p1 = Partition::new(Topic::new("test"), 1);
        let now = Instant::now();
        watchdog.last_seen.insert(p0, now - Duration::from_secs(5));
        watchdog.last_seen.insert(p1, now);

        // Force the throttled check to run on every poll below.
        let force_check = |watchdog: &mut StuckPartitionWatchdog<MockStrategy>| {
            watchdog.last_check = Instant::now() - CHECK_INTERVAL - Duration::from_secs(1);
        };

        // Disabled: a stuck partition is ignored, but polling is still forwarded.
        patch_str_config_for_test("stuck_partition_watchdog_enabled", Some("0"));
        force_check(&mut watchdog);
        watchdog.poll().unwrap();
        assert!(!signal.has_fired());
        assert_eq!(watchdog.next_step.polled, 1);

        // Enabled (1-second timeout so the aged partition counts as stuck): the
        // reconnect signal fires.
        patch_str_config_for_test("stuck_partition_watchdog_enabled", Some("1"));
        patch_str_config_for_test("stuck_partition_watchdog_timeout_seconds", Some("1"));
        force_check(&mut watchdog);
        watchdog.poll().unwrap();
        assert!(signal.has_fired());

        // Subsequent checks must not re-trigger: trigger() returns false once
        // it has already fired.
        force_check(&mut watchdog);
        watchdog.poll().unwrap();
        assert!(!signal.trigger());
    }

    /// `submit` records partition activity and forwards the message downstream.
    #[test]
    fn test_submit_records_activity_and_forwards() {
        let mut watchdog = StuckPartitionWatchdog::new(MockStrategy::new(), ReconnectSignal::new());

        watchdog.submit(make_message(3)).unwrap();

        assert_eq!(watchdog.next_step.submitted, 1);
        assert!(watchdog
            .last_seen
            .contains_key(&Partition::new(Topic::new("test"), 3)));
    }

    /// `terminate` and `join` are forwarded to the next step.
    #[test]
    fn test_terminate_and_join_forwarded() {
        let mut watchdog = StuckPartitionWatchdog::new(MockStrategy::new(), ReconnectSignal::new());

        watchdog.terminate();
        assert!(watchdog.next_step.terminated);

        watchdog.join(None).unwrap();
        assert!(watchdog.next_step.joined);
    }
}
