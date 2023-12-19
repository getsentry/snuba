use std::time::{Duration, Instant};

/// Represents a Deadline to be reached.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    start: Instant,
    duration: Duration,
}

impl Deadline {
    /// Creates a new [`Deadline`].
    pub fn new(duration: Duration) -> Self {
        Self {
            start: Instant::now(),
            duration,
        }
    }

    /// Returns the start since creation.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
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
        self.start = Instant::now();
    }
}
