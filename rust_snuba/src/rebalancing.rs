use crate::runtime_config::get_str_config;
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
    thread::sleep(Duration::from_secs(
        configured_delay_secs - time_elapsed_in_slot,
    ));
}

pub fn get_rebalance_delay_secs(consumer_group: &str) -> Option<u64> {
    if consumer_group == "spans" {
        return Some(15);
    }

    match get_str_config(
        format!(
            "quantized_rebalance_consumer_group_delay_secs__{}",
            consumer_group
        )
        .as_str(),
    ) {
        Ok(delay_secs) => match delay_secs {
            Some(secs) => match secs.parse() {
                Ok(v) => return Some(v),
                Err(_) => return None,
            },
            None => {
                return None;
            }
        },
        Err(_) => {
            return None;
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::prelude::{PyModule, Python};

    fn set_str_config(key: &str, val: &str) {
        let _ = Python::with_gil(|py| {
            let snuba_state = PyModule::import(py, "snuba.state").unwrap();
            snuba_state
                .getattr("set_config")
                .unwrap()
                .call1((key, val))
                .unwrap();
        });
    }

    #[test]
    fn test_delay_config() {
        crate::testutils::initialize_python();
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, None);
        set_str_config(
            "quantized_rebalance_consumer_group_delay_secs__spans",
            "420",
        );
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, Some(15));
        set_str_config(
            "quantized_rebalance_consumer_group_delay_secs__spans",
            "garbage",
        );
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, None);
    }
}
