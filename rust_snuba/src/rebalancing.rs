use sentry_options::options;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn delay_kafka_rebalance(configured_delay_secs: u64) {
    /*
     *  Introduces a configurable delay to the consumer topic
     * subscription and consumer shutdown steps (handled by the
     * StreamProcessor). The idea behind is that by forcing
     * these steps to occur at certain time "ticks" (for example, at
     * every 15 second tick in a minute), we can reduce the number of
     * rebalances that are triggered during a deploy. This means
     * fewer "stop the world rebalancing" occurrences and more time
     * for the consumer group to stabilize and make progress.
     */
    let current_time = SystemTime::now();
    let time_elapsed_in_slot = match current_time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    } % configured_delay_secs;
    tracing::info!(
        "Delaying rebalance by {} seconds",
        configured_delay_secs - time_elapsed_in_slot
    );

    thread::sleep(Duration::from_secs(
        configured_delay_secs - time_elapsed_in_slot,
    ));
}

pub fn get_rebalance_delay_secs(consumer_group: &str) -> Option<u64> {
    // Migrated from the per-group runtime config key
    // `quantized_rebalance_consumer_group_delay_secs__<consumer_group>` to a
    // single `snuba` sentry-options dict keyed by consumer group. A group with
    // no entry (or a non-integer value) yields no delay.
    options("snuba")
        .ok()
        .and_then(|o| o.get("quantized_rebalance_consumer_group_delay_secs").ok())
        .and_then(|v| v.get(consumer_group).and_then(|n| n.as_u64()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentry_options::init_with_schemas;
    use sentry_options::testing::override_options;
    use serde_json::json;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn init_options() {
        INIT.call_once(|| init_with_schemas(&[("snuba", crate::SNUBA_SCHEMA)]).unwrap());
    }

    #[test]
    fn test_delay_config() {
        init_options();

        // A consumer group with no entry in the dict yields no delay.
        assert_eq!(get_rebalance_delay_secs("spans"), None);

        let _guard = override_options(&[(
            "snuba",
            "quantized_rebalance_consumer_group_delay_secs",
            json!({ "spans": 420 }),
        )])
        .unwrap();

        // The configured group reads its delay; an unconfigured one stays None.
        assert_eq!(get_rebalance_delay_secs("spans"), Some(420));
        assert_eq!(get_rebalance_delay_secs("transactions"), None);
    }
}
