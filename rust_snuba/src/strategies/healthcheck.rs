use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

use crate::runtime_config::get_str_config;

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

        if get_str_config("experimental_healthcheck")
            .ok()
            .flatten()
            .unwrap_or("0".to_string())
            == "1".to_string()
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
