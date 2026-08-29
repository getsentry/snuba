use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use sentry_options::options;

use super::HealthFile;

/// Default timeout when the option is missing. Used only by this strategy.
const DEFAULT_PARTITION_STALL_TIMEOUT_SECS: u64 = 300;

struct PartitionState {
    last_submit_at: Instant,
    last_progress_at: Instant,
    last_committed_offset: Option<u64>,
}

/// Per-partition submit/commit times. `healthy()` is a pure read of that state.
struct PartitionTracker {
    partitions: HashMap<Partition, PartitionState>,
}

impl PartitionTracker {
    fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    fn record_submit<TPayload>(&mut self, message: &Message<TPayload>) {
        let now = Instant::now();
        for (partition, _offset) in message.committable() {
            let entry = self
                .partitions
                .entry(partition)
                .or_insert_with(|| PartitionState {
                    last_submit_at: now,
                    last_progress_at: now,
                    last_committed_offset: None,
                });
            entry.last_submit_at = now;
        }
    }

    fn record_commit(&mut self, commit_request: &CommitRequest) {
        let now = Instant::now();
        for (partition, offset) in &commit_request.positions {
            if let Some(entry) = self.partitions.get_mut(partition) {
                let advanced = entry
                    .last_committed_offset
                    .map(|prev| *offset > prev)
                    .unwrap_or(true);
                if advanced {
                    entry.last_progress_at = now;
                    entry.last_committed_offset = Some(*offset);
                }
            } else {
                self.partitions.insert(
                    *partition,
                    PartitionState {
                        last_submit_at: now,
                        last_progress_at: now,
                        last_committed_offset: Some(*offset),
                    },
                );
            }
        }
    }

    fn healthy(&self, now: Instant, timeout: Duration) -> bool {
        for (partition, state) in &self.partitions {
            let inflight = state.last_committed_offset.is_none()
                || state.last_submit_at > state.last_progress_at;
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
                    "partition stall: no commit progress while work is in flight"
                );
                return false;
            }
        }
        true
    }
}

/// Touch the health file unless a partition has in-flight work with no commit
/// advance past `consumer.partition_stall_timeout_secs`. `0` disables stall
/// detection (touch on every poll).
pub struct PartitionStallHealthCheck<Next> {
    next_step: Next,
    file: HealthFile,
    tracker: PartitionTracker,
}

impl<Next> PartitionStallHealthCheck<Next> {
    pub fn new(next_step: Next, path: impl Into<PathBuf>) -> Self {
        Self {
            next_step,
            file: HealthFile::new(path),
            tracker: PartitionTracker::new(),
        }
    }

    /// Read on each poll so the timeout is runtime-tunable. Missing → 300s;
    /// `0` → stall detection off.
    fn stall_timeout(&self) -> Option<Duration> {
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
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for PartitionStallHealthCheck<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();

        match self.stall_timeout() {
            None => self.file.maybe_touch(),
            Some(timeout) => {
                if let Ok(Some(commit_request)) = poll_result.as_ref() {
                    self.tracker.record_commit(commit_request);
                }
                if self.tracker.healthy(Instant::now(), timeout) {
                    self.file.maybe_touch();
                }
            }
        }

        poll_result
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        if self.stall_timeout().is_some() {
            self.tracker.record_submit(&message);
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
    use super::super::testutil::{broker_message, test_partition, MockStrategy};
    use super::PartitionStallHealthCheck;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
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

    #[test]
    fn test_partition_stall_stops_touching_health_file() {
        init_config();
        let _guard =
            override_options(&[("snuba", "consumer.partition_stall_timeout_secs", json!(1))])
                .unwrap();
        let file_path = format!("/tmp/healthcheck_stall_{}", uuid::Uuid::new_v4());
        let partition = test_partition(41);

        let mut health_check: PartitionStallHealthCheck<MockStrategy> =
            PartitionStallHealthCheck::new(MockStrategy::new(false), &file_path);

        health_check.submit(broker_message(partition, 1)).unwrap();
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
        let mut health_check: PartitionStallHealthCheck<MockStrategy> =
            PartitionStallHealthCheck::new(MockStrategy::with_positions(positions), &file_path);

        health_check.submit(broker_message(partition, 9)).unwrap();
        thread::sleep(Duration::from_millis(1100));

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
        let mut health_check: PartitionStallHealthCheck<MockStrategy> =
            PartitionStallHealthCheck::new(MockStrategy::with_positions(positions), &file_path);

        health_check.submit(broker_message(partition, 4)).unwrap();
        let _ = health_check.poll();
        let _ = fs::remove_file(&file_path);

        thread::sleep(Duration::from_millis(1100));

        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "idle partition with no in-flight work must remain healthy"
        );
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_timeout_zero_always_touches() {
        init_config();
        let _guard =
            override_options(&[("snuba", "consumer.partition_stall_timeout_secs", json!(0))])
                .unwrap();
        let file_path = format!("/tmp/healthcheck_disabled_{}", uuid::Uuid::new_v4());
        let partition = test_partition(1);

        let mut health_check: PartitionStallHealthCheck<MockStrategy> =
            PartitionStallHealthCheck::new(MockStrategy::new(false), &file_path);

        health_check.submit(broker_message(partition, 1)).unwrap();
        health_check.submit(broker_message(partition, 2)).unwrap();
        thread::sleep(Duration::from_millis(50));
        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "timeout 0 disables stall detection and still touches on poll"
        );
        let _ = fs::remove_file(&file_path);
    }
}
