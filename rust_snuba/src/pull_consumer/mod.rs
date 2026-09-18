mod entrypoint;
mod factories;
mod pipelines;

pub use entrypoint::pull_consumer;

/// Allowed processors for the fire-and-forget pipeline.
const FIRE_AND_FORGET_PROCESSORS: &[&str] = &[
    "FunctionsMessageProcessor",
    "ProfilesMessageProcessor",
    "QuerylogProcessor",
    "ReplaysProcessor",
    "OutcomesProcessor",
    "ProfileChunksProcessor",
    "LlmProxyCostProcessor",
];

/// Allowed processors for the EAP pipeline.
const EAP_PROCESSORS: &[&str] = &["EAPItemsProcessor"];
