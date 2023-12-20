use std::time::Duration;

/// Represents a Deadline to be reached.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    start: coarsetime::Instant,
    duration: Duration,
}

fn now() -> coarsetime::Instant {
    coarsetime::Instant::now_without_cache_update()
}

impl Deadline {
    /// Creates a new [`Deadline`].
    pub fn new(duration: Duration) -> Self {
        Self {
            start: now(),
            duration,
        }
    }

    /// Returns the start since creation.
    pub fn elapsed(&self) -> Duration {
        now().duration_since(self.start).into()
    }

    /// Checks whether the deadline has elapsed.
    pub fn has_elapsed(&self) -> bool {
        self.elapsed() >= self.duration
    }

    /// Returns the remaining [`Duration`].
    ///
    /// This will be [`Duration::ZERO`] if the deadline has elapsed
    pub fn remaining(&self) -> Duration {
        self.duration.saturating_sub(self.elapsed())
    }

    /// Restarts the deadline with the initial [`Duration`].
    pub fn restart(&mut self) {
        self.start = now();
    }
}
