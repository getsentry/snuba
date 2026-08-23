//! Consumer healthcheck strategy.
//!
//! Touches a file on a timer so Kubernetes liveness probes can kill a stuck
//! consumer. Two optional sentry-options modes refine when the file is touched:
//!
//! - `experimental_healthcheck` (bool): only touch on commit progress, or when
//!   the consumer is idle (no recent submits). Blocks health while work is in
//!   flight without commits.
//! - `consumer.partition_stall_timeout_secs` (integer, default 0 = off): track
//!   last submit and last commit progress per partition. If any partition has
//!   in-flight work (submit after last progress) for longer than the timeout,
//!   stop touching the health file so the liveness probe restarts the pod and
//!   Kafka rebalances the assignment.
//!
//! The stall watchdog is meant for single-partition throughput collapse that
//! still polls and processes slowly: offsets keep moving, but one partition
//! stops committing for long enough to starve GLOBAL subscription watermarks.

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

struct PartitionProgress {
    /// Last time we accepted a message that advanced this partition's
    /// committable offset (work entered the pipeline for this partition).
    last_submit_at: Instant,
    /// Last time a commit request advanced this partition's offset.
    last_progress_at: Instant,
    /// Highest commit offset observed for this partition in this assignment.
    last_committed_offset: Option<u64>,
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

    /// Stall timeout from sentry-options. 0 / missing / non-positive disables
    /// the per-partition watchdog. Read on each poll so it is runtime-tunable.
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

    fn experimental_healthcheck_enabled(&self) -> bool {
        options("snuba")
            .ok()
            .and_then(|o| o.get("experimental_healthcheck").ok())
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
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
                .or_insert(PartitionProgress {
                    last_submit_at: now,
                    // First sighting: treat as healthy until timeout elapses
                    // with no commit while more submits keep arriving.
                    last_progress_at: now,
                    last_committed_offset: None,
                });
            entry.last_submit_at = now;
        }
    }

    fn record_commit_progress(&mut self, commit_request: &CommitRequest) {
        if self.partition_stall_timeout().is_none() {
            return;
        }
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
                }
            } else {
                // Commit without a prior submit in this assignment (e.g. after
                // strategy rebuild). Seed both timestamps so we do not
                // immediately look stalled.
                self.partition_progress.insert(
                    *partition,
                    PartitionProgress {
                        last_submit_at: now,
                        last_progress_at: now,
                        last_committed_offset: Some(*offset),
                    },
                );
            }
        }
    }

    /// Returns true when every partition with in-flight work has committed
    /// within the stall timeout. Idle partitions (no submit after last
    /// progress) are healthy.
    fn partitions_healthy(&self, timeout: Duration) -> bool {
        let now = Instant::now();
        for (partition, state) in &self.partition_progress {
            // In-flight: we have accepted work more recently than we have
            // committed progress for this partition.
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
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for HealthCheck<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();

        if let Ok(Some(commit_request)) = poll_result.as_ref() {
            self.record_commit_progress(commit_request);
        }

        let stall_timeout = self.partition_stall_timeout();
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

        if self.experimental_healthcheck_enabled() {
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
            override_options(&[("snuba", "experimental_healthcheck", json!(true))]).unwrap();
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
            override_options(&[("snuba", "experimental_healthcheck", json!(true))]).unwrap();
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
    fn test_partition_stall_stops_touching_health_file() {
        init_config();
        let _guard =
            override_options(&[("snuba", "consumer.partition_stall_timeout_secs", json!(1))])
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
        let _guard =
            override_options(&[("snuba", "consumer.partition_stall_timeout_secs", json!(1))])
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
        let _guard =
            override_options(&[("snuba", "consumer.partition_stall_timeout_secs", json!(1))])
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
}
