use clap::Parser;
use log;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::processing::strategies::transform::Transform;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory, InvalidMessage,
};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Topic, TopicOrPartition};
use std::time::Duration;

use rust_snuba::settings;
use rust_snuba::storages;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    storages: Vec<String>,

    #[arg(long)]
    settings_path: String,
}

struct Noop {}
impl<T> ProcessingStrategy<T> for Noop where T: Clone {
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }
    fn submit(&mut self, _message: Message<T>) -> Result<(), MessageRejected> {
        Ok(())
    }
    fn close(&mut self) {}
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}

#[derive(Clone)]
struct ClickhouseInsert {}

struct PythonTransformStep {
    pub next_step: Box<dyn ProcessingStrategy<ClickhouseInsert>>,
}

impl ProcessingStrategy<KafkaPayload> for PythonTransformStep {
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        log::info!("processing message");
        let transformed = ClickhouseInsert {};

        self.next_step.submit(Message {
            partition: message.partition,
            offset: message.offset,
            payload: transformed,
            timestamp: message.timestamp,
        })
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.next_step.join(timeout)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    // TODO: Support multiple storages
    let first_storage = args.storages[0].clone();

    log::info!(
        "Starting consumer for {:?} with settings at {}",
        first_storage,
        args.settings_path,
    );
    let settings = settings::Settings::load_from_json(&args.settings_path).unwrap();
    log::info!("Loaded settings: {settings:?}");

    let storage_registry = storages::StorageRegistry::load_all(&settings).unwrap();
    let storage = storage_registry.get(&first_storage).unwrap();

    struct ConsumerStrategyFactory {
        config: KafkaConfig,
    }
    impl ProcessingStrategyFactory<KafkaPayload> for ConsumerStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let transform_step = PythonTransformStep {
                next_step: Box::new(Noop {}),
            };
            Box::new(transform_step)
        }
    }

    let broker_config = settings.get_broker_config(&storage.stream_loader.default_topic)
        .iter()
        .filter_map(|(k, v)| Some((k.to_owned(), v.as_ref()?.to_owned())))
        .collect();
    let config = KafkaConfig::new_config_from_raw(broker_config);

    let consumer = Box::new(KafkaConsumer::new(config.clone()));
    let mut processor = StreamProcessor::new(
        consumer,
        Box::new(ConsumerStrategyFactory { config })
    );

    processor.subscribe(Topic {
        name: settings.kafka_topic_map.get(&storage.stream_loader.default_topic).unwrap_or(&storage.stream_loader.default_topic).to_owned(),
    });

    processor.run().unwrap();
}
