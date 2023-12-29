use std::thread::sleep;
use std::time::{Duration, SystemTime};

pub trait Clock: Send {
    fn time(&self) -> SystemTime;

    fn sleep(&mut self, duration: Duration);
}

pub struct SystemClock {}
impl Clock for SystemClock {
    fn time(&self) -> SystemTime {
        SystemTime::now()
    }

    fn sleep(&mut self, duration: Duration) {
        sleep(duration)
    }
}

pub struct TestingClock {
    time: SystemTime,
}

impl TestingClock {
    pub fn new(now: SystemTime) -> Self {
        Self { time: now }
    }
}

impl Clock for TestingClock {
    fn time(&self) -> SystemTime {
        self.time
    }

    fn sleep(&mut self, duration: Duration) {
        self.time += duration;
    }
}
