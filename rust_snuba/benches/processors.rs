mod utils;

use crate::utils::{functions_payload, profiles_payload, querylog_payload, spans_payload, RUNTIME};

use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{black_box, criterion_group, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
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
use rust_arroyo::utils::metrics::configure_metrics;
use rust_snuba::{
    ClickhouseConfig, ConsumerStrategyFactory, MessageProcessorConfig, StatsDBackend, StorageConfig,
};
use uuid::Uuid;

const MSG_COUNT: usize = 5_000;

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
    let consumer_state = ConsumerState::new(factory, None);
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
        Arc::new(Mutex::new(broker)),
        "test_group".to_string(),
        true,
        &[topic],
        Callbacks(consumer_state.clone()),
    );
    let consumer = Box::new(consumer);

    StreamProcessor::new(consumer, consumer_state)
}

fn run_bench(
    bencher: &mut BenchmarkGroup<WallTime>,
    concurrency: usize,
    make_payload: fn() -> KafkaPayload,
    processor: &str,
    schema: &str,
) {
    bencher
        .throughput(Throughput::Elements(MSG_COUNT as u64))
        .bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &s| {
                b.iter(|| {
                    let mut processor = black_box(create_stream_processor(
                        s,
                        processor,
                        schema,
                        make_payload,
                        MSG_COUNT,
                    ));
                    loop {
                        let res = processor.run_once();
                        if matches!(res, Err(RunError::Poll(ConsumerError::EndOfPartition))) {
                            processor.shutdown();
                            break;
                        }
                    }
                })
            },
        );
}

pub fn spans(c: &mut Criterion) {
    let mut group = c.benchmark_group("spans");
    for concurrency in [1, 4, 16] {
        run_bench(
            &mut group,
            concurrency,
            spans_payload,
            "SpansMessageProcessor",
            "snuba-spans",
        );
    }
    group.finish();
}

pub fn querylog(c: &mut Criterion) {
    let mut group = c.benchmark_group("querylog");
    for concurrency in [1, 4, 16] {
        run_bench(
            &mut group,
            concurrency,
            querylog_payload,
            "QuerylogProcessor",
            "snuba-queries",
        );
    }
    group.finish();
}

pub fn profiles(c: &mut Criterion) {
    let mut group = c.benchmark_group("profiles");
    for concurrency in [1, 4, 16] {
        run_bench(
            &mut group,
            concurrency,
            profiles_payload,
            "ProfilesMessageProcessor",
            "processed-profiles",
        );
    }
    group.finish();
}

pub fn functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("functions");
    for concurrency in [1, 4, 16] {
        run_bench(
            &mut group,
            concurrency,
            functions_payload,
            "FunctionsMessageProcessor",
            "profiles-call-tree",
        );
    }
    group.finish();
}

criterion_group!(benches, spans, querylog, profiles, functions);

fn main() {
    // this sends to nowhere, but because it's UDP we won't error.
    configure_metrics(StatsDBackend::new(
        "127.0.0.1",
        8081,
        "snuba.consumer",
        Default::default(),
    ));

    benches();

    Criterion::default().configure_from_args().final_summary()
}
