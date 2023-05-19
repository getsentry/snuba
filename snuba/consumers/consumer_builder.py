import functools
import logging
from dataclasses import dataclass
from typing import Callable, Optional

from arroyo import Topic
from arroyo.backends.kafka import (
    KafkaConsumer,
    KafkaPayload,
    KafkaProducer,
    build_kafka_consumer_configuration,
)
from arroyo.commit import IMMEDIATE
from arroyo.dlq import DlqLimit, DlqPolicy, KafkaDlqProducer
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategyFactory
from arroyo.utils.profiler import ProcessingStrategyProfilerWrapperFactory
from arroyo.utils.retries import BasicRetryPolicy, RetryPolicy
from confluent_kafka import KafkaError, KafkaException, Producer
from sentry_sdk.api import configure_scope

from snuba.consumers.consumer import (
    CommitLogConfig,
    build_batch_writer,
    process_message,
)
from snuba.consumers.consumer_config import ConsumerConfig
from snuba.consumers.strategy_factory import KafkaConsumerStrategyFactory
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.environment import setup_sentry
from snuba.state import get_config
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaParameters:
    group_id: str
    auto_offset_reset: str
    strict_offset_reset: Optional[bool]
    queued_max_messages_kbytes: int
    queued_min_messages: int


@dataclass(frozen=True)
class ProcessingParameters:
    processes: Optional[int]
    input_block_size: Optional[int]
    output_block_size: Optional[int]


class ConsumerBuilder:
    """
    Simplifies the initialization of a consumer by merging parameters that
    generally come from the command line with defaults that come from the
    dataset class and defaults that come from the settings file.
    """

    def __init__(
        self,
        storage_key: StorageKey,
        consumer_config: ConsumerConfig,
        kafka_params: KafkaParameters,
        processing_params: ProcessingParameters,
        max_batch_size: int,
        max_batch_time_ms: int,
        metrics: MetricsBackend,
        slice_id: Optional[int],
        stats_callback: Optional[Callable[[str], None]] = None,
        commit_retry_policy: Optional[RetryPolicy] = None,
        join_timeout: Optional[int] = None,
        profile_path: Optional[str] = None,
        max_poll_interval_ms: Optional[int] = None,
    ) -> None:
        self.join_timeout = join_timeout
        self.slice_id = slice_id
        self.storage = get_writable_storage(storage_key)
        self.__consumer_config = consumer_config
        self.__kafka_params = kafka_params
        self.consumer_group = kafka_params.group_id

        broker_config = self.__consumer_config.raw_topic.broker_config

        logger.info(f"librdkafka log level: {broker_config.get('log_level', 6)}")

        self.raw_topic = Topic(self.__consumer_config.raw_topic.physical_topic_name)

        self.replacements_topic = (
            Topic(self.__consumer_config.replacements_topic.physical_topic_name)
            if self.__consumer_config.replacements_topic is not None
            else None
        )

        if self.__consumer_config.replacements_topic is not None:
            self.replacements_producer = Producer(
                {
                    **self.__consumer_config.replacements_topic.broker_config,
                    "partitioner": "consistent",
                    "message.max.bytes": 10000000,  # 10MB, default is 1MB)
                }
            )
        else:
            self.replacements_producer = None

        self.commit_log_topic = (
            Topic(self.__consumer_config.commit_log_topic.physical_topic_name)
            if self.__consumer_config.commit_log_topic is not None
            else None
        )

        if self.__consumer_config.commit_log_topic is not None:
            self.commit_log_producer = Producer(
                self.__consumer_config.commit_log_topic.broker_config
            )
        else:
            self.commit_log_producer = None

        self.stats_callback = stats_callback
        self.metrics = metrics
        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.group_id = kafka_params.group_id
        self.auto_offset_reset = kafka_params.auto_offset_reset
        self.strict_offset_reset = kafka_params.strict_offset_reset
        self.queued_max_messages_kbytes = kafka_params.queued_max_messages_kbytes
        self.queued_min_messages = kafka_params.queued_min_messages
        self.processes = processing_params.processes
        self.input_block_size = processing_params.input_block_size
        self.output_block_size = processing_params.output_block_size
        self.__profile_path = profile_path
        self.max_poll_interval_ms = max_poll_interval_ms

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
        self,
        strategy_factory: ProcessingStrategyFactory[KafkaPayload],
        slice_id: Optional[int] = None,
    ) -> StreamProcessor[KafkaPayload]:

        configuration = build_kafka_consumer_configuration(
            {},
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            strict_offset_reset=self.strict_offset_reset,
            queued_max_messages_kbytes=self.queued_max_messages_kbytes,
            queued_min_messages=self.queued_min_messages,
            override_params=self.__consumer_config.raw_topic.broker_config,
        )

        stats_collection_frequency_ms = get_config(
            f"stats_collection_freq_ms_{self.group_id}",
            get_config("stats_collection_freq_ms", 0),
        )

        if stats_collection_frequency_ms and stats_collection_frequency_ms > 0:
            configuration.update(
                {
                    "statistics.interval.ms": stats_collection_frequency_ms,
                    "stats_cb": self.stats_callback,
                }
            )

        if self.max_poll_interval_ms is not None:
            configuration["max.poll.interval.ms"] = self.max_poll_interval_ms

        def log_general_error(e: KafkaError) -> None:
            with configure_scope() as scope:
                scope.fingerprint = [e.code(), e.name()]
                logger.warning(
                    "Error callback from librdKafka %s, %s, %s",
                    e.code(),
                    e.name(),
                    e.str(),
                )

        configuration["error_cb"] = log_general_error

        consumer = KafkaConsumer(
            configuration,
            commit_retry_policy=self.__commit_retry_policy,
        )

        # TODO: Move DLQ into consumer config
        stream_loader = self.storage.get_table_writer().get_stream_loader()

        dlq_config = stream_loader.get_dlq_config()

        if dlq_config is not None:
            dlq_producer = KafkaProducer(
                build_kafka_producer_configuration(
                    dlq_config.topic,
                    slice_id,
                )
            )
            dlq_topic_spec = KafkaTopicSpec(dlq_config.topic)
            resolved_topic = Topic(dlq_topic_spec.get_physical_topic_name(slice_id))

            dlq_policy = DlqPolicy(
                KafkaDlqProducer(dlq_producer, resolved_topic), DlqLimit(), None
            )
        else:
            dlq_policy = None

        return StreamProcessor(
            consumer,
            self.raw_topic,
            strategy_factory,
            IMMEDIATE,
            dlq_policy=dlq_policy,
            join_timeout=self.join_timeout,
        )

    def build_streaming_strategy_factory(
        self,
        slice_id: Optional[int] = None,
    ) -> ProcessingStrategyFactory[KafkaPayload]:
        table_writer = self.storage.get_table_writer()
        stream_loader = table_writer.get_stream_loader()

        logical_topic = stream_loader.get_default_topic_spec().topic

        processor = stream_loader.get_processor()

        if self.commit_log_topic:
            commit_log_config = CommitLogConfig(
                self.commit_log_producer, self.commit_log_topic, self.group_id
            )
        else:
            commit_log_config = None

        strategy_factory: ProcessingStrategyFactory[
            KafkaPayload
        ] = KafkaConsumerStrategyFactory(
            prefilter=stream_loader.get_pre_filter(),
            process_message=functools.partial(
                process_message,
                processor,
                self.consumer_group,
                logical_topic,
            ),
            collector=build_batch_writer(
                table_writer,
                metrics=self.metrics,
                replacements_producer=self.replacements_producer,
                replacements_topic=self.replacements_topic,
                slice_id=slice_id,
                commit_log_config=commit_log_config,
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
                strategy_factory,
                self.__profile_path,
            )

        return strategy_factory

    def flush(self) -> None:
        if self.replacements_producer:
            self.replacements_producer.flush()

    def build_base_consumer(self) -> StreamProcessor[KafkaPayload]:
        """
        Builds the consumer.
        """
        return self.__build_consumer(
            self.build_streaming_strategy_factory(self.slice_id), self.slice_id
        )
