//! Launch the Python message processor and send messages into it, forever. The purpose is to find
//! memory leaks and to see how well it utilizes CPU, not to measure throughput.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;

use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::run_task::RunTask;
use rust_arroyo::processing::strategies::ProcessingStrategy;
use rust_arroyo::types::{Message, Partition, Topic};
use rust_snuba::{MessageProcessorConfig, Noop, PythonTransformStep};
use serde_json::json;

fn main() {
    let step = Noop;

    let output = Arc::new(AtomicUsize::new(0));
    let output2 = output.clone();

    let step = RunTask::new(
        move |_| {
            output2.fetch_add(1, Ordering::Relaxed);
            Ok(())
        },
        step,
    );

    let mut step = PythonTransformStep::new(
        step,
        MessageProcessorConfig {
            python_class_name: "SpansMessageProcessor".into(),
            python_module: "snuba.datasets.processors.spans_processor".into(),
        },
        8,
        Some(256),
    )
    .unwrap();

    for _ in 0..2000000 {
        step.poll().unwrap();
        let message = Message::new_broker_message(
            spans_payload(),
            Partition::new(Topic::new("test"), 1),
            1,
            Utc::now(),
        );
        step.submit(message).unwrap();
    }

    println!("join");

    step.close();
    step.join(None).unwrap();

    println!("{}", output.load(Ordering::Relaxed))
}

fn spans_payload() -> KafkaPayload {
    let now = Utc::now();
    let data = json!({
        "description": "GET /blah",
        "duration_ms": 1000,
        "event_id": "f7d00ab7-ebb8-433b-ba8b-719028bc2f40",
        "exclusive_time_ms": 1000.0,
        "group_raw": "deadbeefdeadbeef",
        "is_segment": false,
        "parent_span_id": "deadbeefdeadbeef",
        "profile_id": "facb4816-3f0f-4b71-9306-c77b5109e7b4",
        "project_id": 1,
        "retention_days": 90,
        "segment_id": "deadbeefdeadbeef",
        "sentry_tags": {
            "action": "GET",
            "domain": "targetdomain.tld:targetport",
            "group": "deadbeefdeadbeef",
            "http.method": "GET",
            "module": "http",
            "op": "http.client",
            "status": "ok",
            "status_code": "200",
            "system": "python",
            "transaction": "/organizations/:orgId/issues/",
            "transaction.method": "GET",
            "transaction.op": "navigation"
        },
        "span_id": "deadbeefdeadbeef",
        "start_timestamp_ms": now.timestamp() * 1000,
        "tags": { "tag1": "value1", "tag2": "123", "tag3": "true" },
        "trace_id": "6f2a27f7-942d-4db1-b406-93524ed7da54"
    });

    let data_bytes = serde_json::to_vec(&data).unwrap();
    KafkaPayload::new(None, None, Some(data_bytes))
}
