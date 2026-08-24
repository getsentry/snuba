//! Consumer healthcheck strategy.
//!
//! Touches a health file so Kubernetes liveness probes can restart a stuck
//! consumer (and Kafka can rebalance its assignment). See
//! `docs/source/architecture/consumer.rst` ("Healthchecks") for the full
//! mode matrix.
//!
//! ## Strategy implementations (`--health-check`)
//!
//! - **arroyo** (default): touch the file on every successful `poll`.
//! - **snuba**: this module. Same file touch, plus optional progress modes
//!   below. Also selected automatically when the partition stall watchdog is
//!   enabled.
//!
//! Both need `--health-check-file`. Without it there is no pod-level check.
//!
//! ## Snuba progress modes (sentry-options)
//!
//! - `consumer.commit_progress_healthcheck` (bool, default false): consumer-level
//!   progress. Touch only when a commit request is observed, or when the
//!   consumer is idle (no recent submits). Unhealthy while work is in flight
//!   without commits. Legacy alias: `experimental_healthcheck`.
//! - `consumer.partition_stall_timeout_secs` (integer, default 0 = off): enable
//!   the per-partition watchdog. Two failure modes:
//!   1. **Hard stall**: a partition has in-flight work (submit after last
//!      commit progress) for longer than the timeout with no commit advance.
//!   2. **Relative slowdown**: over one timeout-sized window, a partition's
//!      commit rate is below `consumer.partition_slow_ratio` of the median
//!      sibling rate on this assignment, while still receiving work.
//!
//! On stall/slowdown failure the health file is not touched, so the liveness
//! probe restarts the pod and Kafka rebalances the assignment.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use sentry_options::options;

const TOUCH_INTERVAL: Duration = Duration::from_secs(1);

/// Default when the option is missing. 0 keeps the watchdog disabled.
const DEFAULT_PARTITION_STALL_TIMEOUT_SECS: u64 = 0;

/// Default relative slowdown ratio when the stall watchdog is enabled and the
/// ratio option is unset. A partition slower than this fraction of the median
/// sibling commit rate is unhealthy. 0.25 matches severe single-partition
/// collapse (e.g. ~1k/s vs ~6k/s peers) without firing on mild skew.
const DEFAULT_PARTITION_SLOW_RATIO: f64 = 0.25;

/// Minimum inclusive assignment-median commit rate (offsets/sec) before
/// relative slowdown can fire. Avoids false positives when the whole
/// assignment is quiet, even if one peer is hotter.
const MIN_MEDIAN_SIBLING_RATE: f64 = 50.0;

struct PartitionProgress {
    /// Last time we accepted a message that advanced this partition's
    /// committable offset (work entered the pipeline for this partition).
    last_submit_at: Instant,
    /// Last time a commit request advanced this partition's offset.
    last_progress_at: Instant,
    /// Highest commit offset observed for this partition in this assignment.
    last_committed_offset: Option<u64>,
    /// Start of the current rate-measurement window.
    rate_window_started_at: Instant,
    /// Committed offset at the start of the current rate window.
    rate_window_start_offset: Option<u64>,
    /// Whether this partition received submits during the current window.
    submits_in_window: bool,
    /// Commit rate (offsets/sec) from the last completed window, if any.
    last_window_rate: Option<f64>,
    /// Whether the last completed window saw submits (active work).
    last_window_had_submits: bool,
}

pub struct HealthCheck<Next> {
    next_step: Next,
    path: PathBuf,
    interval: Duration,
    deadline: SystemTime,
    iterations_since_last_submit: u32,
    /// Per-partition submit/commit progress for the stall watchdog.
    partition_progress: HashMap<Partition, PartitionProgress>,
}

impl<Next> HealthCheck<Next> {
    pub fn new(next_step: Next, path: impl Into<PathBuf>) -> Self {
        let interval = TOUCH_INTERVAL;
        let deadline = SystemTime::now() + interval;

        Self {
            next_step,
            path: path.into(),
            interval,
            deadline,
            iterations_since_last_submit: 0,
            partition_progress: HashMap::new(),
        }
    }

    fn maybe_touch_file(&mut self) {
        let now = SystemTime::now();
        #[cfg(not(test))]
        if now < self.deadline {
            return;
        }

        if let Err(err) = std::fs::File::create(&self.path) {
            let error: &dyn std::error::Error = &err;
            tracing::error!(error);
        }

        counter!("arroyo.processing.strategies.healthcheck.touch");
        self.deadline = now + self.interval;
    }

    /// Stall timeout from sentry-options. 0 / missing disables the watchdog.
    /// Read on each poll so it is runtime-tunable.
    fn partition_stall_timeout(&self) -> Option<Duration> {
        let secs = options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.partition_stall_timeout_secs").ok())
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_PARTITION_STALL_TIMEOUT_SECS);
        if secs == 0 {
            None
        } else {
            Some(Duration::from_secs(secs))
        }
    }

    /// Relative slowdown ratio. When the stall watchdog is on:
    /// - unset → default [`DEFAULT_PARTITION_SLOW_RATIO`]
    /// - `0` → relative check disabled (hard stall only)
    /// - `(0, 1]` → custom ratio
    fn partition_slow_ratio(&self) -> Option<f64> {
        self.partition_stall_timeout()?;
        let ratio = options("snuba")
            .ok()
            .and_then(|o| o.get("consumer.partition_slow_ratio").ok())
            .and_then(|v| v.as_f64())
            .unwrap_or(DEFAULT_PARTITION_SLOW_RATIO);
        if ratio <= 0.0 {
            None
        } else {
            Some(ratio.clamp(0.0, 1.0))
        }
    }

    /// Consumer-level commit-progress mode. Prefer
    /// `consumer.commit_progress_healthcheck`; still honor the legacy
    /// `experimental_healthcheck` alias.
    fn commit_progress_healthcheck_enabled(&self) -> bool {
        let snuba = match options("snuba") {
            Ok(o) => o,
            Err(_) => return false,
        };
        if snuba
            .get("consumer.commit_progress_healthcheck")
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return true;
        }
        snuba
            .get("experimental_healthcheck")
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    fn new_partition_progress(now: Instant) -> PartitionProgress {
        PartitionProgress {
            last_submit_at: now,
            // First sighting: treat as healthy until timeout elapses with no
            // commit while more submits keep arriving.
            last_progress_at: now,
            last_committed_offset: None,
            rate_window_started_at: now,
            rate_window_start_offset: None,
            submits_in_window: false,
            last_window_rate: None,
            last_window_had_submits: false,
        }
    }

    fn record_submit_progress<TPayload>(&mut self, message: &Message<TPayload>) {
        if self.partition_stall_timeout().is_none() {
            return;
        }
        let now = Instant::now();
        for (partition, _offset) in message.committable() {
            let entry = self
                .partition_progress
                .entry(partition)
                .or_insert_with(|| Self::new_partition_progress(now));
            entry.last_submit_at = now;
            entry.submits_in_window = true;
        }
    }

    fn maybe_close_rate_window(entry: &mut PartitionProgress, now: Instant, window: Duration) {
        let elapsed = now.saturating_duration_since(entry.rate_window_started_at);
        if elapsed < window {
            return;
        }

        if let (Some(start_offset), Some(end_offset)) =
            (entry.rate_window_start_offset, entry.last_committed_offset)
        {
            let delta = end_offset.saturating_sub(start_offset) as f64;
            let secs = elapsed.as_secs_f64().max(0.001);
            entry.last_window_rate = Some(delta / secs);
            entry.last_window_had_submits = entry.submits_in_window;
        } else if entry.submits_in_window {
            // Received work but never committed in this window → rate 0.
            entry.last_window_rate = Some(0.0);
            entry.last_window_had_submits = true;
        }

        // Start the next window from the current offset/time.
        entry.rate_window_started_at = now;
        entry.rate_window_start_offset = entry.last_committed_offset;
        entry.submits_in_window = false;
    }

    fn record_commit_progress(&mut self, commit_request: &CommitRequest, window: Duration) {
        let now = Instant::now();
        for (partition, offset) in &commit_request.positions {
            if let Some(entry) = self.partition_progress.get_mut(partition) {
                let advanced = entry
                    .last_committed_offset
                    .map(|prev| *offset > prev)
                    .unwrap_or(true);
                if advanced {
                    entry.last_progress_at = now;
                    entry.last_committed_offset = Some(*offset);
                    if entry.rate_window_start_offset.is_none() {
                        entry.rate_window_start_offset = Some(*offset);
                        entry.rate_window_started_at = now;
                    }
                }
                Self::maybe_close_rate_window(entry, now, window);
            } else {
                // Commit without a prior submit in this assignment (e.g. after
                // strategy rebuild). Seed so we do not immediately look stalled.
                let mut entry = Self::new_partition_progress(now);
                entry.last_committed_offset = Some(*offset);
                entry.rate_window_start_offset = Some(*offset);
                self.partition_progress.insert(*partition, entry);
            }
        }

        // Close windows for partitions that did not appear in this commit so
        // relative rates stay fresh even when a partition stops committing.
        for entry in self.partition_progress.values_mut() {
            Self::maybe_close_rate_window(entry, now, window);
        }
    }

    /// Hard stall: in-flight work with no commit progress past the timeout.
    fn hard_stall_healthy(&self, timeout: Duration) -> bool {
        let now = Instant::now();
        for (partition, state) in &self.partition_progress {
            let inflight = state.last_submit_at > state.last_progress_at;
            if !inflight {
                continue;
            }
            let stalled_for = now.saturating_duration_since(state.last_progress_at);
            if stalled_for > timeout {
                counter!("arroyo.processing.strategies.healthcheck.partition_stall");
                tracing::error!(
                    partition = %partition,
                    stalled_for_secs = stalled_for.as_secs(),
                    timeout_secs = timeout.as_secs(),
                    "partition stall watchdog: no commit progress while work is in flight"
                );
                return false;
            }
        }
        true
    }

    /// Median of a non-empty rate slice. Caller must pass at least one value.
    fn median_rate(rates: &[f64]) -> f64 {
        debug_assert!(!rates.is_empty());
        let mut sorted = rates.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        if n % 2 == 1 {
            sorted[n / 2]
        } else {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        }
    }

    /// Relative slowdown: one partition's completed-window commit rate is far
    /// below the median of its *siblings* on this assignment (leave-one-out).
    ///
    /// Quiet floor uses the *inclusive* median of all active rates so a single
    /// hot partition cannot force mostly-quiet peers into the slowdown check.
    /// The ratio comparison still excludes the candidate so a 2-partition
    /// collapse (~1k vs ~6k) fires at the default 0.25 ratio.
    fn relative_slowdown_healthy(&self, ratio: f64) -> bool {
        // Active partitions: completed a window while receiving submits.
        let active_rates: Vec<(Partition, f64)> = self
            .partition_progress
            .iter()
            .filter_map(|(partition, state)| {
                if state.last_window_had_submits {
                    state.last_window_rate.map(|rate| (*partition, rate))
                } else {
                    None
                }
            })
            .collect();

        // Need at least two active partitions to compare siblings.
        if active_rates.len() < 2 {
            return true;
        }

        // Inclusive quiet floor for the whole assignment.
        let all_rates: Vec<f64> = active_rates.iter().map(|(_, rate)| *rate).collect();
        let assignment_median = Self::median_rate(&all_rates);
        if assignment_median < MIN_MEDIAN_SIBLING_RATE {
            return true;
        }

        for (partition, rate) in &active_rates {
            let sibling_rates: Vec<f64> = active_rates
                .iter()
                .filter(|(other, _)| other != partition)
                .map(|(_, sibling_rate)| *sibling_rate)
                .collect();
            if sibling_rates.is_empty() {
                continue;
            }

            let median_sibling = Self::median_rate(&sibling_rates);
            let threshold = median_sibling * ratio;
            if *rate < threshold {
                counter!("arroyo.processing.strategies.healthcheck.partition_slow");
                tracing::error!(
                    partition = %partition,
                    partition_rate = rate,
                    median_sibling_rate = median_sibling,
                    assignment_median = assignment_median,
                    ratio = ratio,
                    threshold = threshold,
                    "partition stall watchdog: commit rate far below sibling median"
                );
                return false;
            }
        }
        true
    }

    fn partitions_healthy(&mut self, timeout: Duration) -> bool {
        // Close any windows that matured without a commit poll path.
        let now = Instant::now();
        for entry in self.partition_progress.values_mut() {
            Self::maybe_close_rate_window(entry, now, timeout);
        }

        if !self.hard_stall_healthy(timeout) {
            return false;
        }
        if let Some(ratio) = self.partition_slow_ratio() {
            if !self.relative_slowdown_healthy(ratio) {
                return false;
            }
        }
        true
    }
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for HealthCheck<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();

        let stall_timeout = self.partition_stall_timeout();
        if let (Ok(Some(commit_request)), Some(window)) = (poll_result.as_ref(), stall_timeout) {
            self.record_commit_progress(commit_request, window);
        }

        let partitions_ok = match stall_timeout {
            Some(timeout) => self.partitions_healthy(timeout),
            None => true,
        };

        if !partitions_ok {
            // Do not touch the health file. K8s liveness will fail and restart
            // the pod, which forces a rebalance of the stalled assignment.
            self.iterations_since_last_submit += 1;
            return poll_result;
        }

        if self.commit_progress_healthcheck_enabled() {
            // If we are receiving a commit request, it means we are making progress and this can be considered a healthy state
            if let Ok(Some(_commit_request)) = poll_result.as_ref() {
                self.maybe_touch_file();
            }

            // If we aren't submitting, it means we are not processing messages and we consider this a healthy state
            if self.iterations_since_last_submit > 0 {
                self.maybe_touch_file();
            }

            self.iterations_since_last_submit += 1;
        } else {
            self.maybe_touch_file();
            self.iterations_since_last_submit += 1;
        }
        poll_result
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        self.record_submit_progress(&message);
        self.iterations_since_last_submit = 0;
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
    use super::HealthCheck;
    use chrono::Utc;
    use sentry_arroyo::processing::strategies::{
        CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
    };
    use sentry_arroyo::types::{Message, Partition, Topic};
    use sentry_options::testing::override_options;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::Once;
    use std::thread;
    use std::time::Duration;

    static INIT: Once = Once::new();
    fn init_config() {
        INIT.call_once(|| crate::init_sentry_options().unwrap());
    }

    // Mock strategy that can be configured to return commit requests
    struct MockStrategy {
        return_commit_request: bool,
        commit_positions: HashMap<Partition, u64>,
    }

    impl MockStrategy {
        fn new(return_commit_request: bool) -> Self {
            Self {
                return_commit_request,
                commit_positions: HashMap::new(),
            }
        }

        fn with_positions(positions: HashMap<Partition, u64>) -> Self {
            Self {
                return_commit_request: true,
                commit_positions: positions,
            }
        }
    }

    impl ProcessingStrategy<()> for MockStrategy {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            if self.return_commit_request {
                Ok(Some(CommitRequest {
                    positions: self.commit_positions.clone(),
                }))
            } else {
                Ok(None)
            }
        }

        fn submit(&mut self, _message: Message<()>) -> Result<(), SubmitError<()>> {
            Ok(())
        }

        fn terminate(&mut self) {}

        fn join(
            &mut self,
            _timeout: Option<Duration>,
        ) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    fn test_partition(index: u16) -> Partition {
        Partition::new(Topic::new("test-topic"), index)
    }

    fn broker_message(partition: Partition, offset: u64) -> Message<()> {
        Message::new_broker_message((), partition, offset, Utc::now())
    }

    #[test]
    fn test_file_created_when_making_progress() {
        // Setup
        init_config();
        let _guard =
            override_options(&[("snuba", "consumer.commit_progress_healthcheck", json!(true))])
                .unwrap();
        let file_path = format!("/tmp/healthcheck_test_{}", uuid::Uuid::new_v4());

        // Create a mock strategy that returns a commit request
        let mock_strategy = MockStrategy::new(true);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);

        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "Health check file should be created when making progress"
        );
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_not_making_progress() {
        // Setup
        init_config();
        let _guard =
            override_options(&[("snuba", "consumer.commit_progress_healthcheck", json!(true))])
                .unwrap();
        let file_path = format!("/tmp/healthcheck_test_{}", uuid::Uuid::new_v4());

        // Create a mock strategy that doesn't return a commit request
        let mock_strategy = MockStrategy::new(false);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);

        let _ = health_check.poll(); // iterations_since_last_submit becomes 1

        assert!(
            !Path::new(&file_path).exists(),
            "Health check file should not be created when we don't have a commit request"
        );

        let _ = health_check.poll();

        // Assert
        assert!(
            Path::new(&file_path).exists(),
            "Health check file should be created when not receiving messages (we haven't called submit) and we don't have a commit request"
        );

        // Cleanup
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_legacy_experimental_healthcheck_alias() {
        init_config();
        let _guard =
            override_options(&[("snuba", "experimental_healthcheck", json!(true))]).unwrap();
        let file_path = format!("/tmp/healthcheck_legacy_{}", uuid::Uuid::new_v4());
        let mock_strategy = MockStrategy::new(true);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);
        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "legacy experimental_healthcheck alias should enable commit-progress mode"
        );
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_partition_stall_stops_touching_health_file() {
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.partition_stall_timeout_secs", json!(1)),
            // Disable relative check so this test is hard-stall only.
            ("snuba", "consumer.partition_slow_ratio", json!(0.0)),
        ])
        .unwrap();
        let file_path = format!("/tmp/healthcheck_stall_{}", uuid::Uuid::new_v4());
        let partition = test_partition(41);

        // No commits from the next step — only submits, so progress never advances
        // after the initial seed on first submit.
        let mock_strategy = MockStrategy::new(false);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);

        // First submit seeds last_progress_at = now (healthy).
        health_check.submit(broker_message(partition, 1)).unwrap();
        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "should be healthy immediately after first submit"
        );
        let _ = fs::remove_file(&file_path);

        // More submits keep last_submit_at ahead of last_progress_at.
        health_check.submit(broker_message(partition, 2)).unwrap();

        // Wait past the 1s stall timeout without any commit progress.
        thread::sleep(Duration::from_millis(1100));

        let _ = health_check.poll();
        assert!(
            !Path::new(&file_path).exists(),
            "stalled partition with in-flight work must not touch the health file"
        );
    }

    #[test]
    fn test_partition_commit_clears_stall() {
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.partition_stall_timeout_secs", json!(1)),
            ("snuba", "consumer.partition_slow_ratio", json!(0.0)),
        ])
        .unwrap();
        let file_path = format!("/tmp/healthcheck_recover_{}", uuid::Uuid::new_v4());
        let partition = test_partition(7);

        let mut positions = HashMap::new();
        positions.insert(partition, 10);
        let mock_strategy = MockStrategy::with_positions(positions);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);

        health_check.submit(broker_message(partition, 9)).unwrap();
        thread::sleep(Duration::from_millis(1100));

        // poll returns a commit request for the partition → progress advances
        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "commit progress should keep the consumer healthy even after the stall window"
        );
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_idle_partition_is_healthy() {
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.partition_stall_timeout_secs", json!(1)),
            ("snuba", "consumer.partition_slow_ratio", json!(0.0)),
        ])
        .unwrap();
        let file_path = format!("/tmp/healthcheck_idle_{}", uuid::Uuid::new_v4());
        let partition = test_partition(3);

        let mut positions = HashMap::new();
        positions.insert(partition, 5);
        let mock_strategy = MockStrategy::with_positions(positions);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);

        // Submit then commit so last_progress_at >= last_submit_at (idle).
        health_check.submit(broker_message(partition, 4)).unwrap();
        let _ = health_check.poll(); // records commit progress
        let _ = fs::remove_file(&file_path);

        thread::sleep(Duration::from_millis(1100));

        // Idle (caught up) partition must stay healthy with no further submits.
        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "idle partition with no in-flight work must remain healthy"
        );
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_watchdog_disabled_by_default() {
        init_config();
        // Explicitly set timeout to 0 (disabled). Do not leave a previous
        // override from another test hanging around.
        let _guard =
            override_options(&[("snuba", "consumer.partition_stall_timeout_secs", json!(0))])
                .unwrap();
        let file_path = format!("/tmp/healthcheck_disabled_{}", uuid::Uuid::new_v4());
        let partition = test_partition(1);

        let mock_strategy = MockStrategy::new(false);
        let mut health_check: HealthCheck<MockStrategy> =
            HealthCheck::new(mock_strategy, &file_path);

        health_check.submit(broker_message(partition, 1)).unwrap();
        health_check.submit(broker_message(partition, 2)).unwrap();
        thread::sleep(Duration::from_millis(50));
        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "with watchdog disabled, default healthcheck still touches on poll"
        );
        let _ = fs::remove_file(&file_path);
    }

    /// Controllable mock: commit positions can change between polls via shared map.
    struct MutableCommitMock {
        positions: std::sync::Arc<std::sync::Mutex<HashMap<Partition, u64>>>,
    }

    impl ProcessingStrategy<()> for MutableCommitMock {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            let positions = self.positions.lock().unwrap().clone();
            if positions.is_empty() {
                Ok(None)
            } else {
                Ok(Some(CommitRequest { positions }))
            }
        }

        fn submit(&mut self, _message: Message<()>) -> Result<(), SubmitError<()>> {
            Ok(())
        }

        fn terminate(&mut self) {}

        fn join(
            &mut self,
            _timeout: Option<Duration>,
        ) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    #[test]
    fn test_relative_slowdown_stops_touching_health_file() {
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.partition_stall_timeout_secs", json!(1)),
            // Default 0.25: leave-one-out sibling median must catch 100/s vs 6000/s.
            ("snuba", "consumer.partition_slow_ratio", json!(0.25)),
        ])
        .unwrap();
        let file_path = format!("/tmp/healthcheck_slow_{}", uuid::Uuid::new_v4());
        let fast = test_partition(1);
        let slow = test_partition(2);

        let positions = std::sync::Arc::new(std::sync::Mutex::new(HashMap::from([
            // Seed both partitions at the same offset so the first commit only
            // opens the rate window.
            (fast, 1000u64),
            (slow, 1000u64),
        ])));
        let mock = MutableCommitMock {
            positions: positions.clone(),
        };
        let mut health_check: HealthCheck<MutableCommitMock> = HealthCheck::new(mock, &file_path);

        // Mark both partitions active (receiving work).
        health_check.submit(broker_message(fast, 999)).unwrap();
        health_check.submit(broker_message(slow, 999)).unwrap();
        let _ = health_check.poll(); // opens rate windows at offset 1000
        let _ = fs::remove_file(&file_path);

        // Fast partition advances a lot; slow barely moves — still "progress"
        // so hard-stall does not fire, but relative rate should.
        {
            let mut pos = positions.lock().unwrap();
            pos.insert(fast, 1000 + 6000);
            pos.insert(slow, 1000 + 100);
        }

        // Keep both active with submits during the window.
        health_check.submit(broker_message(fast, 7000)).unwrap();
        health_check.submit(broker_message(slow, 1100)).unwrap();

        thread::sleep(Duration::from_millis(1100));
        let _ = health_check.poll(); // closes windows and evaluates rates

        assert!(
            !Path::new(&file_path).exists(),
            "partition much slower than sibling median must not touch the health file"
        );
    }

    #[test]
    fn test_balanced_rates_stay_healthy() {
        init_config();
        let _guard = override_options(&[
            ("snuba", "consumer.partition_stall_timeout_secs", json!(1)),
            ("snuba", "consumer.partition_slow_ratio", json!(0.5)),
        ])
        .unwrap();
        let file_path = format!("/tmp/healthcheck_balanced_{}", uuid::Uuid::new_v4());
        let p0 = test_partition(10);
        let p1 = test_partition(11);

        let positions = std::sync::Arc::new(std::sync::Mutex::new(HashMap::from([
            (p0, 100u64),
            (p1, 100u64),
        ])));
        let mock = MutableCommitMock {
            positions: positions.clone(),
        };
        let mut health_check: HealthCheck<MutableCommitMock> = HealthCheck::new(mock, &file_path);

        health_check.submit(broker_message(p0, 99)).unwrap();
        health_check.submit(broker_message(p1, 99)).unwrap();
        let _ = health_check.poll();
        let _ = fs::remove_file(&file_path);

        // Both partitions advance similarly.
        {
            let mut pos = positions.lock().unwrap();
            pos.insert(p0, 100 + 5000);
            pos.insert(p1, 100 + 4800);
        }
        health_check.submit(broker_message(p0, 5100)).unwrap();
        health_check.submit(broker_message(p1, 4900)).unwrap();

        thread::sleep(Duration::from_millis(1100));
        let _ = health_check.poll();

        assert!(
            Path::new(&file_path).exists(),
            "balanced sibling rates must remain healthy"
        );
        let _ = fs::remove_file(&file_path);
    }
}
