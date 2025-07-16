mod config;
mod consumer;
mod factory;
mod factory_v2;
mod logging;
mod metrics;
mod processors;
mod rebalancing;
mod runtime_config;
mod strategies;
mod types;

use pyo3::prelude::*;

#[pymodule]
fn rust_snuba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(consumer::consumer, m)?)?;
    m.add_function(wrap_pyfunction!(consumer::process_message, m)?)?;
    Ok(())
}

// FIXME(swatinem):
// These are exported for the benchmarks.
// Ideally, we would have a normal rust crate we can use in examples and benchmarks,
// plus a pyo3 specific crate as `cdylib`.
pub use config::{
    BrokerConfig, ClickhouseConfig, EnvConfig, MessageProcessorConfig, ProcessorConfig,
    StorageConfig, TopicConfig,
};
pub use factory::ConsumerStrategyFactory;
pub use factory_v2::ConsumerStrategyFactoryV2;
pub use metrics::statsd::StatsDBackend;
pub use processors::{ProcessingFunction, ProcessingFunctionType, PROCESSORS};
pub use strategies::noop::Noop;
pub use strategies::python::PythonTransformStep;
pub use types::KafkaMessageMetadata;

#[cfg(test)]
mod testutils;
