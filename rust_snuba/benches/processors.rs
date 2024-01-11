use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::DateTime;
use criterion::measurement::WallTime;
use criterion::{black_box, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::local::broker::LocalBroker;
use rust_arroyo::backends::local::LocalConsumer;
use rust_arroyo::backends::storages::memory::MemoryMessageStorage;
use rust_arroyo::backends::ConsumerError;
use rust_arroyo::metrics;
use rust_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use rust_arroyo::processing::strategies::ProcessingStrategyFactory;
use rust_arroyo::processing::{Callbacks, ConsumerState, RunError, StreamProcessor};
use rust_arroyo::types::{Partition, Topic};
use rust_arroyo::utils::clock::SystemClock;
use rust_snuba::{
    get_processing_function, ClickhouseConfig, ConsumerStrategyFactory, EnvConfig,
    KafkaMessageMetadata, MessageProcessorConfig, ProcessorConfig, StatsDBackend, StorageConfig,
};
use uuid::Uuid;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const MSG_COUNT: usize = 5_000;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

static PROCESSOR_CONFIG: Lazy<ProcessorConfig> = Lazy::new(ProcessorConfig::default);

fn create_factory(
    concurrency: usize,
    schema: &str,
) -> Box<dyn ProcessingStrategyFactory<KafkaPayload>> {
    let processor_name = processor_for_schema(schema);
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
            python_class_name: processor_name.into(),
            python_module: "test".into(),
        },
    };

    let processing_concurrency =
        ConcurrencyConfig::with_runtime(concurrency, RUNTIME.handle().to_owned());
    let clickhouse_concurrency =
        ConcurrencyConfig::with_runtime(concurrency, RUNTIME.handle().to_owned());
    let commitlog_concurrency =
        ConcurrencyConfig::with_runtime(concurrency, RUNTIME.handle().to_owned());
    let factory = ConsumerStrategyFactory::new(
        storage,
        EnvConfig::default(),
        schema.into(),
        1_000,
        Duration::from_millis(10),
        true,
        processing_concurrency,
        clickhouse_concurrency,
        commitlog_concurrency,
        None,
        true,
        None,
        false,
        None,
        "test-group".to_owned(),
        Topic::new("test"),
    );
    Box::new(factory)
}

fn create_stream_processor(
    concurrency: usize,
    schema: &str,
    messages: usize,
) -> StreamProcessor<KafkaPayload> {
    let factory = create_factory(concurrency, schema);
    let consumer_state = ConsumerState::new(factory, None);
    let topic = Topic::new("test");
    let partition = Partition::new(topic, 0);

    let storage: MemoryMessageStorage<KafkaPayload> = Default::default();
    let clock = SystemClock {};
    let mut broker = LocalBroker::new(Box::new(storage), Box::new(clock));
    broker.create_topic(topic, 1).unwrap();

    let schema = sentry_kafka_schemas::get_schema(schema, None).unwrap();
    let payloads = schema.examples();
    for payload in payloads.iter().cycle().take(messages) {
        let payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
        broker.produce(&partition, payload).unwrap();
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

fn run_fn_bench(bencher: &mut BenchmarkGroup<WallTime>, schema: &str) {
    let processor_name = processor_for_schema(schema);
    let processor_fn = get_processing_function(processor_name).unwrap();
    let metadata = KafkaMessageMetadata {
        partition: 0,
        offset: 1,
        timestamp: DateTime::from(SystemTime::now()),
    };
    let schema = sentry_kafka_schemas::get_schema(schema, None).unwrap();
    let payloads = schema.examples();

    bencher
        .warm_up_time(Duration::from_millis(500))
        .throughput(Throughput::Elements(payloads.len() as u64))
        .bench_function(BenchmarkId::from_parameter("-"), |b| {
            b.iter(|| {
                for payload in payloads {
                    let payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
                    let processed =
                        processor_fn(payload, metadata.clone(), &PROCESSOR_CONFIG).unwrap();
                    black_box(processed);
                }
            })
        });
}

fn run_processor_bench(bencher: &mut BenchmarkGroup<WallTime>, concurrency: usize, schema: &str) {
    bencher
        .throughput(Throughput::Elements(MSG_COUNT as u64))
        .warm_up_time(Duration::from_millis(500))
        .sample_size(10)
        .bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &s| {
                b.iter(|| {
                    let mut processor = black_box(create_stream_processor(s, schema, MSG_COUNT));
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

macro_rules! define_benches {
    ($c:ident: $($name:ident => $schema:literal,)+) => {
        $({
            let mut group = $c.benchmark_group(stringify!($name));
            run_fn_bench(&mut group, $schema);
            for concurrency in [1, 4, 16] {
                run_processor_bench(&mut group, concurrency, $schema);
            }
            group.finish();
        })+
    };
}

pub fn processor_for_schema(schema: &str) -> &str {
    match schema {
        "snuba-spans" => "SpansMessageProcessor",
        "snuba-queries" => "QuerylogProcessor",
        "processed-profiles" => "ProfilesMessageProcessor",
        "profiles-call-tree" => "FunctionsMessageProcessor",
        "ingest-replay-events" => "ReplaysProcessor",
        "snuba-generic-metrics" => "MetricsSummariesMessageProcessor",
        "outcomes" => "OutcomesProcessor",
        _ => todo!("need to add new schemas and processors"),
    }
}

fn main() {
    // this sends to nowhere, but because it's UDP we won't error.
    metrics::init(StatsDBackend::new("127.0.0.1", 8081, "snuba.consumer")).unwrap();

    let mut c = Criterion::default().configure_from_args();

    define_benches!(c:
        spans => "snuba-spans",
        querylog => "snuba-queries",
        profiles => "processed-profiles",
        functions => "profiles-call-tree",
        replays => "ingest-replay-events",
        // FIXME: the schema does not really match the metrics summaries
        // metrics => "snuba-generic-metrics",
        outcomes => "outcomes",
    );

    c.final_summary()
}
