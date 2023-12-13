use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use divan::counter::ItemsCount;
use once_cell::sync::Lazy;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::local::broker::LocalBroker;
use rust_arroyo::backends::local::LocalConsumer;
use rust_arroyo::backends::storages::memory::MemoryMessageStorage;
use rust_arroyo::backends::ConsumerError;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::strategies::ProcessingStrategyFactory;
use rust_arroyo::processing::{Callbacks, ConsumerState, RunError, StreamProcessor};
use rust_arroyo::types::{Partition, Topic};
use rust_arroyo::utils::clock::SystemClock;
use rust_arroyo::utils::metrics::{configure_metrics, Metrics};
use rust_snuba::{
    ClickhouseConfig, ConsumerStrategyFactory, MessageProcessorConfig, StorageConfig,
};
use uuid::Uuid;

fn main() {
    divan::main();
}

const MSG_COUNT: usize = 5_000;
static METRICS_INIT: Once = Once::new();

#[divan::bench(consts = [1, 4, 16])]
fn functions<const N: usize>(bencher: divan::Bencher) {
    run_bench(
        bencher,
        N,
        functions_payload,
        "FunctionsMessageProcessor",
        "profiles-call-tree",
    );
}

#[divan::bench(consts = [1, 4, 16])]
fn profiles<const N: usize>(bencher: divan::Bencher) {
    run_bench(
        bencher,
        N,
        profiles_payload,
        "ProfilesMessageProcessor",
        "processed-profiles",
    );
}

#[divan::bench(consts = [1, 4, 16])]
fn querylog<const N: usize>(bencher: divan::Bencher) {
    run_bench(
        bencher,
        N,
        querylog_payload,
        "QuerylogProcessor",
        "snuba-queries",
    );
}

#[divan::bench(consts = [1, 4, 16])]
fn spans<const N: usize>(bencher: divan::Bencher) {
    run_bench(
        bencher,
        N,
        spans_payload,
        "SpansMessageProcessor",
        "snuba-spans",
    );
}

fn run_bench(
    bencher: divan::Bencher,
    concurrency: usize,
    make_payload: fn() -> KafkaPayload,
    processor: &str,
    schema: &str,
) {
    METRICS_INIT.call_once(|| {
        #[derive(Debug)]
        struct Noop;
        impl Metrics for Noop {
            fn increment(&self, _key: &str, _value: i64, _tags: Option<HashMap<&str, &str>>) {}
            fn gauge(&self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}
            fn timing(&self, _key: &str, _value: u64, _tags: Option<HashMap<&str, &str>>) {}
        }

        configure_metrics(Noop)
    });
    bencher
        .counter(ItemsCount::new(MSG_COUNT))
        .with_inputs(|| {
            create_stream_processor(concurrency, processor, schema, make_payload, MSG_COUNT)
        })
        .bench_local_values(|mut processor| {
            loop {
                let res = processor.run_once();
                if matches!(res, Err(RunError::Poll(ConsumerError::EndOfPartition))) {
                    // FIXME: this pretty much means that we *polled* the partition to the end,
                    // it does not mean that we actually *committed* everything
                    // (aka we finished actually processing everything).
                    break;
                }
            }
        });
}

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

fn create_factory(
    concurrency: usize,
    processor: &str,
    schema: &str,
) -> Box<dyn ProcessingStrategyFactory<KafkaPayload>> {
    let storage = StorageConfig {
        name: "test".into(),
        clickhouse_table_name: "test".into(),
        clickhouse_cluster: ClickhouseConfig {
            host: "test".into(),
            port: 1234,
            http_port: 1234,
            user: "test".into(),
            password: "test".into(),
            database: "test".into(),
        },
        message_processor: MessageProcessorConfig {
            python_class_name: processor.into(),
            python_module: "test".into(),
        },
    };

    let processing_concurrency =
        ConcurrencyConfig::with_runtime(concurrency, RUNTIME.handle().to_owned());
    let clickhouse_concurrency =
        ConcurrencyConfig::with_runtime(concurrency, RUNTIME.handle().to_owned());
    let factory = ConsumerStrategyFactory::new(
        storage,
        schema.into(),
        1_000,
        Duration::from_millis(10),
        true,
        processing_concurrency,
        clickhouse_concurrency,
        None,
        true,
        None,
    );
    Box::new(factory)
}

fn create_stream_processor(
    concurrency: usize,
    processor: &str,
    schema: &str,
    make_payload: fn() -> KafkaPayload,
    messages: usize,
) -> StreamProcessor<KafkaPayload> {
    let factory = create_factory(concurrency, processor, schema);
    let consumer_state = Arc::new(Mutex::new(ConsumerState::new(factory, None)));
    let topic = Topic::new("test");
    let partition = Partition::new(topic, 0);

    let storage: MemoryMessageStorage<KafkaPayload> = Default::default();
    let clock = SystemClock {};
    let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));
    broker.create_topic(topic, 1).unwrap();

    for _ in 0..messages {
        broker.produce(&partition, make_payload()).unwrap();
    }

    let consumer = LocalConsumer::new(
        Uuid::nil(),
        broker,
        "test_group".to_string(),
        true,
        &[topic],
        Callbacks(consumer_state.clone()),
    );
    let consumer = Box::new(consumer);

    StreamProcessor::new(consumer, consumer_state)
}

fn functions_payload() -> KafkaPayload {
    let data = r#"{
        "project_id": 22,
        "profile_id": "7329158c39964fbb9ec57c20cf4a2bb8",
        "transaction_name": "vroom-vroom",
        "timestamp": 1694447692,
        "functions": [
            {
                "fingerprint": 123,
                "function": "foo",
                "package": "bar",
                "in_app": true,
                "self_times_ns": [1, 2, 3]
            },
            {
                "fingerprint": 456,
                "function": "baz",
                "package": "qux",
                "in_app": false,
                "self_times_ns": [4, 5, 6]
            }
        ],
        "platform": "python",
        "environment": "prod",
        "release": "foo@1.0.0",
        "dist": "1",
        "transaction_op": "http.server",
        "transaction_status": "ok",
        "http_method": "GET",
        "browser_name": "Chrome",
        "device_class": 2,
        "retention_days": 30
    }"#;
    KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()))
}

fn profiles_payload() -> KafkaPayload {
    let data = r#"{
        "android_api_level": null,
        "architecture": "aarch64",
        "device_classification": "high",
        "device_locale": "fr_FR",
        "device_manufacturer": "Pierre",
        "device_model": "ThePierrePhone",
        "device_os_build_number": "13",
        "device_os_name": "PierreOS",
        "device_os_version": "47",
        "duration_ns": 50000000000,
        "environment": "production",
        "organization_id": 1,
        "platform": "python",
        "profile_id": "a6cd859435584c3391412390168dcb93",
        "project_id": 1,
        "received": 1694357860,
        "retention_days": 30,
        "trace_id": "40300eb2e77c46908de27f4603befa45",
        "transaction_id": "b716a5ee27db49dcbb534dcca61a9df8",
        "transaction_name": "lets-get-ready-to-party",
        "version_code": "1337",
        "version_name": "v42.0.0"
    }"#;
    KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()))
}

fn querylog_payload() -> KafkaPayload {
    let data = r#"{
        "request": {
            "id": "24a78d10a0134f2aa6367ba2a393b504",
            "body": {
            "legacy": true,
            "query": "MATCH (events) SELECT count() AS `count`, min(timestamp) AS `first_seen`, max(timestamp) AS `last_seen` BY tags_key, tags_value WHERE timestamp >= toDateTime('2023-02-08T21:07:12.769001') AND timestamp < toDateTime('2023-02-08T21:12:39.015094') AND project_id IN tuple(1) AND project_id IN tuple(1) AND group_id IN tuple(5) ORDER BY count DESC LIMIT 4 BY tags_key",
            "dataset": "events",
            "app_id": "legacy",
            "parent_api": "/api/0/issues|groups/{issue_id}/tags/"
            },
            "referrer": "tagstore.__get_tag_keys_and_top_values",
            "team": "<unknown>",
            "feature": "<unknown>",
            "app_id": "legacy"
        },
        "dataset": "events",
        "entity": "events",
        "start_timestamp": 1675919232,
        "end_timestamp": 1675919559,
        "query_list": [
            {
            "sql": "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> (x, y)), tags.key, tags.value)) AS snuba_all_tags), 1) AS _snuba_tags_key), (tupleElement(snuba_all_tags, 2) AS _snuba_tags_value), (count() AS _snuba_count), (min((timestamp AS _snuba_timestamp)) AS _snuba_first_seen), (max(_snuba_timestamp) AS _snuba_last_seen) FROM errors_local PREWHERE in((group_id AS _snuba_group_id), tuple(5)) WHERE equals(deleted, 0) AND greaterOrEquals(_snuba_timestamp, toDateTime('2023-02-08T21:07:12', 'Universal')) AND less(_snuba_timestamp, toDateTime('2023-02-08T21:12:39', 'Universal')) AND in((project_id AS _snuba_project_id), tuple(1)) AND in(_snuba_project_id, tuple(1)) GROUP BY _snuba_tags_key, _snuba_tags_value ORDER BY _snuba_count DESC LIMIT 4 BY _snuba_tags_key LIMIT 1000 OFFSET 0",
            "sql_anonymized": "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> (x, y)), tags.key, tags.value)) AS snuba_all_tags), -1337) AS _snuba_tags_key), (tupleElement(snuba_all_tags, -1337) AS _snuba_tags_value), (count() AS _snuba_count), (min((timestamp AS _snuba_timestamp)) AS _snuba_first_seen), (max(_snuba_timestamp) AS _snuba_last_seen) FROM errors_local PREWHERE in((group_id AS _snuba_group_id), tuple(-1337)) WHERE equals(deleted, -1337) AND greaterOrEquals(_snuba_timestamp, toDateTime('2023-02-08T21:07:12', 'Universal')) AND less(_snuba_timestamp, toDateTime('2023-02-08T21:12:39', 'Universal')) AND in((project_id AS _snuba_project_id), tuple(-1337)) AND in(_snuba_project_id, tuple(-1337)) GROUP BY _snuba_tags_key, _snuba_tags_value ORDER BY _snuba_count DESC LIMIT 4 BY _snuba_tags_key LIMIT 1000 OFFSET 0",
            "start_timestamp": 1675919232,
            "end_timestamp": 1675919559,
            "stats": {
                "clickhouse_table": "errors_local",
                "final": false,
                "referrer": "tagstore.__get_tag_keys_and_top_values",
                "sample": null,
                "table_rate": 0.6,
                "table_concurrent": 1,
                "project_rate": 0.6333333333333333,
                "project_concurrent": 1,
                "consistent": false,
                "result_rows": 22,
                "result_cols": 5,
                "query_id": "9079915acbacff0804ed45c72b865024"
            },
            "status": "success",
            "trace_id": "8377f280e7eb4754a7f20df73ce2cf37",
            "profile": {
                "time_range": null,
                "table": "errors_local",
                "all_columns": [
                "errors_local.deleted",
                "errors_local.group_id",
                "errors_local.project_id",
                "errors_local.tags.key",
                "errors_local.tags.value",
                "errors_local.timestamp"
                ],
                "multi_level_condition": false,
                "where_profile": {
                "columns": [
                    "errors_local.deleted",
                    "errors_local.project_id",
                    "errors_local.timestamp"
                ],
                "mapping_cols": []
                },
                "groupby_cols": ["errors_local.tags.key", "errors_local.tags.value"],
                "array_join_cols": ["errors_local.tags.key", "errors_local.tags.value"]
            },
            "result_profile": {
                "bytes": 1305,
                "blocks": 1,
                "rows": 22,
                "elapsed": 0.009863138198852539
            },
            "request_status": "success",
            "slo": "for"
            }
        ],
        "status": "success",
        "request_status": "success",
        "slo": "for",
        "timing": {
            "timestamp": 1675890758,
            "duration_ms": 55,
            "marks_ms": {
            "cache_get": 2,
            "cache_set": 6,
            "execute": 10,
            "get_configs": 0,
            "prepare_query": 15,
            "rate_limit": 5,
            "validate_schema": 15
            },
            "tags": {}
        },
        "projects": [1],
        "snql_anonymized": "MATCH Entity(events) SELECT tags_key, tags_value, (count() AS count), (min(timestamp) AS first_seen), (max(timestamp) AS last_seen) GROUP BY tags_key, tags_value WHERE greaterOrEquals(timestamp, toDateTime('$S')) AND less(timestamp, toDateTime('$S')) AND in(project_id, tuple(-1337)) AND in(project_id, tuple(-1337)) AND in(group_id, tuple(-1337)) ORDER BY count DESC LIMIT 4 BY tags_key LIMIT 1000 OFFSET 0"
    }"#;
    KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()))
}

fn spans_payload() -> KafkaPayload {
    let data = r#"{
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
        "start_timestamp_ms": 1691105878720,
        "tags": { "tag1": "value1", "tag2": "123", "tag3": "true" },
        "trace_id": "6f2a27f7-942d-4db1-b406-93524ed7da54"
    }"#;
    KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()))
}
