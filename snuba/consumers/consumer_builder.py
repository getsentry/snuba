import functools
import logging
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from arroyo import Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategyFactory
from arroyo.processing.strategies.streaming import KafkaConsumerStrategyFactory
from arroyo.utils.profiler import ProcessingStrategyProfilerWrapperFactory
from arroyo.utils.retries import BasicRetryPolicy, RetryPolicy
from confluent_kafka import KafkaError, KafkaException, Producer

from snuba.consumers.consumer import (
    build_batch_writer,
    build_mock_batch_writer,
    process_message,
)
from snuba.consumers.snapshot_worker import SnapshotProcessor
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.environment import setup_sentry
from snuba.processor import MessageProcessor
from snuba.snapshots import SnapshotId
from snuba.stateful_consumer.control_protocol import TransactionData
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import (
    build_kafka_consumer_configuration,
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.kafka_consumer_with_commit_log import (
    KafkaConsumerWithCommitLog,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaParameters:
    raw_topic: Optional[str]
    replacements_topic: Optional[str]
    bootstrap_servers: Optional[Sequence[str]]
    group_id: str
    commit_log_topic: Optional[str]
    auto_offset_reset: str
    queued_max_messages_kbytes: int
    queued_min_messages: int
    stats_collection_frequency_ms: Optional[int]


@dataclass(frozen=True)
class ProcessingParameters:
    processes: Optional[int]
    input_block_size: Optional[int]
    output_block_size: Optional[int]


@dataclass(frozen=True)
class MockParameters:
    avg_write_latency: int
    std_deviation: int


class ConsumerBuilder:
    """
    Simplifies the initialization of a consumer by merging parameters that
    generally come from the command line with defaults that come from the
    dataset class and defaults that come from the settings file.
    """

    def __init__(
        self,
        storage_key: StorageKey,
        kafka_params: KafkaParameters,
        processing_params: ProcessingParameters,
        max_batch_size: int,
        max_batch_time_ms: int,
        metrics: MetricsBackend,
        stats_callback: Optional[Callable[[str], None]] = None,
        commit_retry_policy: Optional[RetryPolicy] = None,
        profile_path: Optional[str] = None,
        mock_parameters: Optional[MockParameters] = None,
    ) -> None:
        self.storage = get_writable_storage(storage_key)
        self.bootstrap_servers = kafka_params.bootstrap_servers
        topic = (
            self.storage.get_table_writer()
            .get_stream_loader()
            .get_default_topic_spec()
            .topic
        )

        self.broker_config = get_default_kafka_configuration(
            topic, bootstrap_servers=kafka_params.bootstrap_servers
        )
        logger.info(f"librdkafka log level: {self.broker_config.get('log_level', 6)}")
        self.producer_broker_config = build_kafka_producer_configuration(
            topic,
            bootstrap_servers=kafka_params.bootstrap_servers,
            override_params={
                "partitioner": "consistent",
                "message.max.bytes": 50000000,  # 50MB, default is 1MB
            },
        )

        stream_loader = self.storage.get_table_writer().get_stream_loader()

        self.raw_topic: Topic
        if kafka_params.raw_topic is not None:
            self.raw_topic = Topic(kafka_params.raw_topic)
        else:
            self.raw_topic = Topic(stream_loader.get_default_topic_spec().topic_name)

        self.replacements_topic: Optional[Topic]
        if kafka_params.replacements_topic is not None:
            self.replacements_topic = Topic(kafka_params.replacements_topic)
        else:
            replacement_topic_spec = stream_loader.get_replacement_topic_spec()
            if replacement_topic_spec is not None:
                self.replacements_topic = Topic(replacement_topic_spec.topic_name)
            else:
                self.replacements_topic = None

        self.commit_log_topic: Optional[Topic]
        if kafka_params.commit_log_topic is not None:
            self.commit_log_topic = Topic(kafka_params.commit_log_topic)
        else:
            commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()
            if commit_log_topic_spec is not None:
                self.commit_log_topic = Topic(commit_log_topic_spec.topic_name)
            else:
                self.commit_log_topic = None

        if kafka_params.stats_collection_frequency_ms is not None:
            self.stats_collection_frequency_ms = (
                kafka_params.stats_collection_frequency_ms
            )
            assert stats_callback is not None
            self.stats_callback = stats_callback
        else:
            self.stats_collection_frequency_ms = 0

        # XXX: This can result in a producer being built in cases where it's
        # not actually required.
        self.producer = Producer(self.producer_broker_config)

        self.metrics = metrics
        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.group_id = kafka_params.group_id
        self.auto_offset_reset = kafka_params.auto_offset_reset
        self.queued_max_messages_kbytes = kafka_params.queued_max_messages_kbytes
        self.queued_min_messages = kafka_params.queued_min_messages
        self.processes = processing_params.processes
        self.input_block_size = processing_params.input_block_size
        self.output_block_size = processing_params.output_block_size
        self.__profile_path = profile_path
        self.__mock_parameters = mock_parameters

        if commit_retry_policy is None:
            commit_retry_policy = BasicRetryPolicy(
                3,
                1,
                lambda e: isinstance(e, KafkaException)
                and e.args[0].code()
                in (
                    KafkaError.REQUEST_TIMED_OUT,
                    KafkaError.NOT_COORDINATOR,
                    KafkaError._WAIT_COORD,
                ),
            )

        self.__commit_retry_policy = commit_retry_policy

    def __build_consumer(
        self, strategy_factory: ProcessingStrategyFactory[KafkaPayload]
    ) -> StreamProcessor[KafkaPayload]:
        configuration = build_kafka_consumer_configuration(
            self.storage.get_table_writer()
            .get_stream_loader()
            .get_default_topic_spec()
            .topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            queued_max_messages_kbytes=self.queued_max_messages_kbytes,
            queued_min_messages=self.queued_min_messages,
        )

        if self.stats_collection_frequency_ms > 0:
            configuration.update(
                {
                    "statistics.interval.ms": self.stats_collection_frequency_ms,
                    "stats_cb": self.stats_callback,
                }
            )

        if self.commit_log_topic is None:
            consumer = KafkaConsumer(
                configuration, commit_retry_policy=self.__commit_retry_policy,
            )
        else:
            consumer = KafkaConsumerWithCommitLog(
                configuration,
                producer=self.producer,
                commit_log_topic=self.commit_log_topic,
                commit_retry_policy=self.__commit_retry_policy,
            )

        return StreamProcessor(consumer, self.raw_topic, strategy_factory)

    def __build_streaming_strategy_factory(
        self,
        processor_wrapper: Optional[
            Callable[[MessageProcessor], MessageProcessor]
        ] = None,
    ) -> ProcessingStrategyFactory[KafkaPayload]:
        table_writer = self.storage.get_table_writer()
        stream_loader = table_writer.get_stream_loader()

        processor = stream_loader.get_processor()
        if processor_wrapper is not None:
            processor = processor_wrapper(processor)

        strategy_factory: ProcessingStrategyFactory[
            KafkaPayload
        ] = KafkaConsumerStrategyFactory(
            stream_loader.get_pre_filter(),
            functools.partial(process_message, processor),
            build_batch_writer(
                table_writer,
                metrics=self.metrics,
                replacements_producer=(
                    self.producer if self.replacements_topic is not None else None
                ),
                replacements_topic=self.replacements_topic,
            )
            if self.__mock_parameters is None
            else build_mock_batch_writer(
                self.storage,
                bool(self.replacements_topic),
                self.metrics,
                self.__mock_parameters.avg_write_latency,
                self.__mock_parameters.std_deviation,
            ),
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms / 1000.0,
            processes=self.processes,
            input_block_size=self.input_block_size,
            output_block_size=self.output_block_size,
            initialize_parallel_transform=setup_sentry,
        )

        if self.__profile_path is not None:
            strategy_factory = ProcessingStrategyProfilerWrapperFactory(
                strategy_factory, self.__profile_path,
            )

        return strategy_factory

    def build_base_consumer(self) -> StreamProcessor[KafkaPayload]:
        """
        Builds the consumer.
        """
        return self.__build_consumer(self.__build_streaming_strategy_factory())

    def build_snapshot_aware_consumer(
        self, snapshot_id: SnapshotId, transaction_data: TransactionData,
    ) -> StreamProcessor[KafkaPayload]:
        """
        Builds the consumer with a processor that is able to handle snapshots.
        """
        return self.__build_consumer(
            self.__build_streaming_strategy_factory(
                lambda processor: SnapshotProcessor(
                    processor, snapshot_id, transaction_data
                )
            )
        )
