use std::{thread::sleep, time::Duration};

use chrono::{TimeDelta, Utc};
use sentry_arroyo::{
    processing::strategies::{MessageRejected, SubmitError},
    types::Message,
};

/// Add static delay so that the mutations consumer always lags behind snuba-eap-spans.
///
/// The "Synchronizer" should eventually become a full replacement for SynchronizedConsumer, hold
/// its own kafka consumer for the commit-log, and be useful beyond EAP, just implemented as a
/// strategy. Right now it just ensures that there is a significant amount of constant consumer
/// lag.
pub struct Synchronizer {
    pub min_delay: TimeDelta,
}

impl Synchronizer {
    pub fn process_message<T>(
        &mut self,
        message: Message<T>,
    ) -> Result<Message<T>, SubmitError<T>> {
        if let Some(ts) = message.timestamp() {
            if Utc::now() - ts < self.min_delay {
                // constant backpressure causes arroyo to consumer 100% CPU. work around it with
                // 10s delay. doing that unconditionally in arroyo is not appropriate for every
                // usecase IMO.
                sleep(Duration::from_secs(10));
                return Err(MessageRejected { message }.into());
            }
        }

        Ok(message)
    }
}
