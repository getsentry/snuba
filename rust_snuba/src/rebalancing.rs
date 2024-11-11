use crate::runtime_config::get_str_config;

/*
fn delay_kafka_rebalance(configured_delay: i32) {

}


fn quantized_rebalance_signal_handler() {

}
*/

fn get_rebalance_delay_secs(consumer_group: &str) -> Option<i32> {
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
        assert_eq!(delay_secs, Some(420));
        set_str_config(
            "quantized_rebalance_consumer_group_delay_secs__spans",
            "garbage",
        );
        let delay_secs = get_rebalance_delay_secs("spans");
        assert_eq!(delay_secs, None);
    }
}
