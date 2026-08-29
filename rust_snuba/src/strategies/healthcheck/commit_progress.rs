use std::path::PathBuf;
use std::time::Duration;

use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

use super::HealthFile;

/// Touch the health file when a commit request is observed, or when the
/// consumer is idle (no recent submits). Unhealthy while work is in flight
/// without commits. Consumer-level: one stuck partition can stay healthy if
/// siblings still commit.
pub struct CommitProgressHealthCheck<Next> {
    next_step: Next,
    file: HealthFile,
    iterations_since_last_submit: u32,
}

impl<Next> CommitProgressHealthCheck<Next> {
    pub fn new(next_step: Next, path: impl Into<PathBuf>) -> Self {
        Self {
            next_step,
            file: HealthFile::new(path),
            iterations_since_last_submit: 0,
        }
    }
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for CommitProgressHealthCheck<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();

        if let Ok(Some(_commit_request)) = poll_result.as_ref() {
            self.file.maybe_touch();
        }

        if self.iterations_since_last_submit > 0 {
            self.file.maybe_touch();
        }

        self.iterations_since_last_submit += 1;
        poll_result
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
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
    use super::super::testutil::MockStrategy;
    use super::CommitProgressHealthCheck;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_file_created_when_making_progress() {
        let file_path = format!("/tmp/healthcheck_test_{}", uuid::Uuid::new_v4());
        let mut health_check: CommitProgressHealthCheck<MockStrategy> =
            CommitProgressHealthCheck::new(MockStrategy::new(true), &file_path);

        let _ = health_check.poll();
        assert!(
            Path::new(&file_path).exists(),
            "Health check file should be created when making progress"
        );
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_not_making_progress() {
        let file_path = format!("/tmp/healthcheck_test_{}", uuid::Uuid::new_v4());
        let mut health_check: CommitProgressHealthCheck<MockStrategy> =
            CommitProgressHealthCheck::new(MockStrategy::new(false), &file_path);

        let _ = health_check.poll();

        assert!(
            !Path::new(&file_path).exists(),
            "Health check file should not be created when we don't have a commit request"
        );

        let _ = health_check.poll();

        assert!(
            Path::new(&file_path).exists(),
            "Health check file should be created when not receiving messages (we haven't called submit) and we don't have a commit request"
        );

        let _ = fs::remove_file(&file_path);
    }
}
