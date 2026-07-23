use crate::config::ProcessorConfig;
use crate::processors::utils::StringToIntDatetime;
use crate::types::{InsertBatch, KafkaMessageMetadata};
use anyhow::Context;
use schemars::JsonSchema;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};

pub fn process_message(
    payload: KafkaPayload,
    _metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let msg: LlmProxyCost = serde_json::from_slice(payload_bytes)?;

    InsertBatch::from_rows([msg], None)
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct LlmProxyCost {
    #[serde(default)]
    timestamp: StringToIntDatetime,
    #[serde(default)]
    org_id: u64,
    #[serde(default)]
    project_id: u64,
    #[serde(default)]
    feature: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    call_type: String,
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
    #[serde(default)]
    cache_read_tokens: u32,
    #[serde(default)]
    cache_write_tokens: u32,
    #[serde(default)]
    total_cost_usd: f64,
    #[serde(default)]
    input_cost_usd: f64,
    #[serde(default)]
    output_cost_usd: f64,
    #[serde(default)]
    cache_read_cost_usd: f64,
    #[serde(default)]
    cache_write_cost_usd: f64,
    #[serde(default)]
    litellm_cost_usd: f64,
    #[serde(default)]
    is_long_context: u8,
    #[serde(default)]
    is_regional: u8,
    #[serde(default)]
    response_time_ms: f64,
    #[serde(default)]
    litellm_call_id: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use std::time::SystemTime;

    #[test]
    fn test_llm_proxy_cost() {
        let data = r#"{
            "timestamp": "2026-06-30T22:15:30.123456Z",
            "org_id": 1,
            "project_id": 42,
            "feature": "autofix",
            "model": "claude-sonnet-4-6",
            "region": "us-east5",
            "call_type": "chat.completion",
            "prompt_tokens": 15000,
            "completion_tokens": 2500,
            "cache_read_tokens": 8000,
            "cache_write_tokens": 3000,
            "total_cost_usd": 0.0385,
            "input_cost_usd": 0.015,
            "output_cost_usd": 0.0125,
            "cache_read_cost_usd": 0.004,
            "cache_write_cost_usd": 0.003,
            "litellm_cost_usd": 0.035,
            "is_long_context": 0,
            "is_regional": 1,
            "response_time_ms": 4523.7,
            "litellm_call_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        }"#;
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn schema() {
        use crate::processors::tests::run_schema_type_test;
        run_schema_type_test::<LlmProxyCost>("snuba-llm-proxy-cost", None);
    }
}
