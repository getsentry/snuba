use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

use sentry_options::options;

const TOUCH_INTERVAL: Duration = Duration::from_secs(1);

pub struct HealthCheck<Next> {
    next_step: Next,
    path: PathBuf,
    interval: Duration,
    deadline: SystemTime,
    iterations_since_last_submit: u32,
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
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for HealthCheck<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();

        if options("snuba")
            .ok()
            .and_then(|o| o.get("experimental_healthcheck").ok())
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
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
        }
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
    use super::HealthCheck;
    use sentry_arroyo::processing::strategies::{
        CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
    };
    use sentry_arroyo::types::Message;
    use sentry_options::testing::override_options;
    use sentry_options::Options;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::Once;
    use std::time::Duration;

    static INIT: Once = Once::new();
    fn init_config() {
        INIT.call_once(|| {
            Options::builder()
                .with_schemas(&[("snuba", crate::SNUBA_SCHEMA)])
                .init()
                .unwrap()
        });
    }

    // Mock strategy that can be configured to return commit requests
    struct MockStrategy {
        return_commit_request: bool,
    }

    impl MockStrategy {
        fn new(return_commit_request: bool) -> Self {
            Self {
                return_commit_request,
            }
        }
    }

    impl ProcessingStrategy<()> for MockStrategy {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            if self.return_commit_request {
                Ok(Some(CommitRequest {
                    positions: HashMap::new(),
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
}
