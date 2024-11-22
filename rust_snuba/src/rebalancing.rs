use crate::runtime_config;
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
    runtime_config::get_str_config(
        format!(
            "quantized_rebalance_consumer_group_delay_secs__{}",
            consumer_group
        )
        .as_str(),
    )
    .ok()??
    .parse()
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delay_config() {
        // teardown, even when the test fails
        let _guard = scopeguard::guard((), |_| {
            runtime_config::patch_str_config_for_test(
                "quantized_rebalance_consumer_group_delay_secs__spans",
                None,
            );
        });

        runtime_config::patch_str_config_for_test(
            "quantized_rebalance_consumer_group_delay_secs__spans",
            None,
        );
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, None);
        runtime_config::patch_str_config_for_test(
            "quantized_rebalance_consumer_group_delay_secs__spans",
            Some("420"),
        );
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, Some(420));
        runtime_config::patch_str_config_for_test(
            "quantized_rebalance_consumer_group_delay_secs__spans",
            Some("garbage"),
        );
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, None);
    }
}
