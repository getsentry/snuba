//! Rust-consumer healthcheck strategies.
//!
//! Each `--health-check` value is a separate [`ProcessingStrategy`] that owns
//! the Kubernetes health file. The probe restarts the pod when the file is
//! not recreated. See `docs/source/architecture/consumer.rst` ("Healthchecks").
//!
//! - **arroyo** (default): stock arroyo strategy, touch on every successful `poll`.
//! - **commit-progress**: [`CommitProgressHealthCheck`] — touch on commit or idle.
//! - **partition-stall**: [`PartitionStallHealthCheck`] — touch unless a partition
//!   has in-flight work with no commit past `consumer.partition_stall_timeout_secs`.
//!
//! `snuba` is a deprecated alias for `commit-progress`. All of these require
//! `--health-check-file`.

mod commit_progress;
mod partition_stall;

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use sentry_arroyo::counter;

pub use commit_progress::CommitProgressHealthCheck;
pub use partition_stall::PartitionStallHealthCheck;

const TOUCH_INTERVAL: Duration = Duration::from_secs(1);

/// Touches the Kubernetes health file at most once per [`TOUCH_INTERVAL`].
struct HealthFile {
    path: PathBuf,
    interval: Duration,
    deadline: SystemTime,
}

impl HealthFile {
    fn new(path: impl Into<PathBuf>) -> Self {
        let interval = TOUCH_INTERVAL;
        Self {
            path: path.into(),
            interval,
            deadline: SystemTime::now() + interval,
        }
    }

    fn maybe_touch(&mut self) {
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
}

#[cfg(test)]
mod testutil {
    use std::collections::HashMap;
    use std::time::Duration;

    use chrono::Utc;
    use sentry_arroyo::processing::strategies::{
        CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
    };
    use sentry_arroyo::types::{Message, Partition, Topic};

    pub struct MockStrategy {
        pub return_commit_request: bool,
        pub commit_positions: HashMap<Partition, u64>,
    }

    impl MockStrategy {
        pub fn new(return_commit_request: bool) -> Self {
            Self {
                return_commit_request,
                commit_positions: HashMap::new(),
            }
        }

        pub fn with_positions(positions: HashMap<Partition, u64>) -> Self {
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

    pub fn test_partition(index: u16) -> Partition {
        Partition::new(Topic::new("test-topic"), index)
    }

    pub fn broker_message(partition: Partition, offset: u64) -> Message<()> {
        Message::new_broker_message((), partition, offset, Utc::now())
    }
}
