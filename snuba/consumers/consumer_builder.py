import functools
import logging
from dataclasses import dataclass
from typing import Optional

from arroyo.backends.kafka import (
    KafkaConsumer,
    KafkaPayload,
    KafkaProducer,
    build_kafka_configuration,
    build_kafka_consumer_configuration,
)
from arroyo.commit import IMMEDIATE
from arroyo.dlq import DlqLimit, DlqPolicy, KafkaDlqProducer, NoopDlqProducer
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategyFactory
from arroyo.types import Topic
from arroyo.utils.profiler import ProcessingStrategyProfilerWrapperFactory
from confluent_kafka import KafkaError, Producer
from sentry_sdk.api import configure_scope

from snuba.consumers.consumer import (
    CommitLogConfig,
    build_batch_writer,
    process_message,
)
from snuba.consumers.consumer_config import ConsumerConfig
from snuba.consumers.dlq import DlqInstruction, DlqReplayPolicy
from snuba.consumers.strategy_factory import KafkaConsumerStrategyFactory
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_sentry
from snuba.utils.metrics import MetricsBackend

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

    Multiple storage consumer is not currently supported via consumer builder,
    supports building a single storage consumer only.
    """

    def __init__(
        self,
        consumer_config: ConsumerConfig,
        kafka_params: KafkaParameters,
        processing_params: ProcessingParameters,
        max_batch_size: int,
        max_batch_time_ms: int,
        max_insert_batch_size: Optional[int],
        max_insert_batch_time_ms: Optional[int],
        metrics: MetricsBackend,
        slice_id: Optional[int],
        join_timeout: Optional[float],
        enforce_schema: bool,
        profile_path: Optional[str] = None,
        max_poll_interval_ms: Optional[int] = None,
        health_check_file: Optional[str] = None,
        group_instance_id: Optional[str] = None,
        skip_write: bool = False,
    ) -> None:
        assert len(consumer_config.storages) == 1, "Only one storage supported"
        storage_key = StorageKey(consumer_config.storages[0].name)

        self.join_timeout = join_timeout
        self.slice_id = slice_id
        self.storage = get_writable_storage(storage_key)
        self.__consumer_config = consumer_config
        self.__kafka_params = kafka_params
        self.consumer_group = kafka_params.group_id
        self.__enforce_schema = enforce_schema
        self.__skip_write = skip_write

        broker_config = build_kafka_consumer_configuration(
            self.__consumer_config.raw_topic.broker_config,
            self.__kafka_params.group_id,
        )

        logger.info(f"librdkafka log level: {broker_config.get('log_level', 6)}")

        self.raw_topic = Topic(self.__consumer_config.raw_topic.physical_topic_name)

        self.replacements_topic = (
            Topic(self.__consumer_config.replacements_topic.physical_topic_name)
            if self.__consumer_config.replacements_topic is not None
            else None
        )

        if self.__consumer_config.replacements_topic is not None:
            self.replacements_producer = Producer(
                build_kafka_configuration(
                    self.__consumer_config.replacements_topic.broker_config,
                    override_params={
                        "partitioner": "consistent",
                        "message.max.bytes": 10000000,  # 10MB, default is 1MB)
                    },
                )
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
                build_kafka_configuration(
                    self.__consumer_config.commit_log_topic.broker_config
                )
            )
        else:
            self.commit_log_producer = None

        self.metrics = metrics
        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.max_insert_batch_size = max_insert_batch_size
        self.max_insert_batch_time_ms = max_insert_batch_time_ms
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
        self.health_check_file = health_check_file
        self.group_instance_id = group_instance_id

        self.dlq_producer: Optional[KafkaProducer] = None

    def __build_consumer(
        self,
        strategy_factory: ProcessingStrategyFactory[KafkaPayload],
        input_topic: Topic,
        dlq_policy: Optional[DlqPolicy[KafkaPayload]],
    ) -> StreamProcessor[KafkaPayload]:

        configuration = build_kafka_consumer_configuration(
            self.__consumer_config.raw_topic.broker_config,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            strict_offset_reset=self.strict_offset_reset,
            queued_max_messages_kbytes=self.queued_max_messages_kbytes,
            queued_min_messages=self.queued_min_messages,
        )

        if self.max_poll_interval_ms is not None:
            configuration["max.poll.interval.ms"] = self.max_poll_interval_ms
            # HACK: If the max poll interval is less than 45 seconds, set the session timeout
            # to the same. (it's default is 45 seconds and it must be <= to max.poll.interval.ms)
            if self.max_poll_interval_ms < 45000:
                configuration["session.timeout.ms"] = self.max_poll_interval_ms

        if self.group_instance_id is not None:
            configuration["group.instance.id"] = self.group_instance_id

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
        )

        return StreamProcessor(
            consumer,
            input_topic,
            strategy_factory,
            # We already debounce commits by batch sizes, ONCE_PER_SECOND just adds more lag.
            IMMEDIATE,
            dlq_policy=dlq_policy,
            join_timeout=self.join_timeout,
        )

    def build_streaming_strategy_factory(
        self,
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
                self.__enforce_schema,
            ),
            collector=build_batch_writer(
                table_writer,
                metrics=self.metrics,
                replacements_producer=self.replacements_producer,
                replacements_topic=self.replacements_topic,
                slice_id=self.slice_id,
                commit_log_config=commit_log_config,
            ),
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms / 1000.0,
            max_insert_batch_size=self.max_insert_batch_size,
            max_insert_batch_time=self.max_insert_batch_time_ms
            and self.max_insert_batch_time_ms / 1000.0,
            processes=self.processes,
            input_block_size=self.input_block_size,
            output_block_size=self.output_block_size,
            initialize_parallel_transform=setup_sentry,
            health_check_file=self.health_check_file,
            skip_write=self.__skip_write,
        )

        if self.__profile_path is not None:
            strategy_factory = ProcessingStrategyProfilerWrapperFactory(
                strategy_factory,
                self.__profile_path,
            )

        return strategy_factory

    def build_dlq_strategy_factory(
        self, instruction: DlqInstruction
    ) -> ProcessingStrategyFactory[KafkaPayload]:
        """
        Similar to build_streaming_strategy_factory with 2 differences:
        - uses ExitAfterNMessages instead of the standard CommitOffsets strategy
        - never commits to the commit log (the commit log should always be monotonic)
        """
        table_writer = self.storage.get_table_writer()
        stream_loader = table_writer.get_stream_loader()

        logical_topic = stream_loader.get_default_topic_spec().topic

        processor = stream_loader.get_processor()

        # DLQ consumer never writes to the commit log
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
                self.__enforce_schema,
            ),
            collector=build_batch_writer(
                table_writer,
                metrics=self.metrics,
                replacements_producer=self.replacements_producer,
                replacements_topic=self.replacements_topic,
                slice_id=self.slice_id,
                commit_log_config=commit_log_config,
            ),
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms / 1000.0,
            max_insert_batch_size=self.max_insert_batch_size,
            max_insert_batch_time=self.max_insert_batch_time_ms
            and self.max_insert_batch_time_ms / 1000.0,
            processes=self.processes,
            input_block_size=self.input_block_size,
            output_block_size=self.output_block_size,
            max_messages_to_process=instruction.max_messages_to_process,
            initialize_parallel_transform=setup_sentry,
        )

        return strategy_factory

    def flush(self) -> None:
        if self.replacements_producer:
            self.replacements_producer.flush()

        if self.commit_log_producer:
            self.commit_log_producer.flush()

        if self.dlq_producer:
            self.dlq_producer.close()

    def build_base_consumer(self) -> StreamProcessor[KafkaPayload]:
        """
        Builds the consumer.
        """
        return self.__build_consumer(
            self.build_streaming_strategy_factory(),
            self.raw_topic,
            self.__build_default_dlq_policy(),
        )

    def build_dlq_consumer(
        self, instruction: DlqInstruction
    ) -> StreamProcessor[KafkaPayload]:
        """
        Dlq consumer. Same as the base consumer but exits after `max_messages_to_process`
        and applies the user defined DLQ policy.
        """
        if not instruction.is_valid():
            raise ValueError("Invalid DLQ instruction")

        if instruction.policy == DlqReplayPolicy.REINSERT_DLQ:
            dlq_policy = self.__build_default_dlq_policy()
        elif instruction.policy == DlqReplayPolicy.DROP_INVALID_MESSAGES:
            dlq_policy = DlqPolicy(
                NoopDlqProducer(),
                None,
                None,
            )
        elif instruction.policy == DlqReplayPolicy.STOP_ON_ERROR:
            dlq_policy = None
        else:
            raise ValueError("Invalid DLQ policy")

        dlq_topic = self.__consumer_config.dlq_topic
        assert dlq_topic is not None

        return self.__build_consumer(
            self.build_dlq_strategy_factory(instruction),
            Topic(dlq_topic.physical_topic_name),
            dlq_policy,
        )

    def __build_default_dlq_policy(self) -> Optional[DlqPolicy[KafkaPayload]]:
        """
        Default DLQ policy applies to the base consumer or the DLQ consumer when
        the selected policy is re-insert to DLQ.
        """
        if self.__consumer_config.dlq_topic is not None:
            self.dlq_producer = KafkaProducer(
                build_kafka_configuration(
                    self.__consumer_config.dlq_topic.broker_config
                )
            )

            dlq_policy = DlqPolicy(
                KafkaDlqProducer(
                    self.dlq_producer,
                    Topic(self.__consumer_config.dlq_topic.physical_topic_name),
                ),
                DlqLimit(
                    max_invalid_ratio=0.01,
                    max_consecutive_count=1000,
                ),
                None,
            )
        else:
            dlq_policy = None

        return dlq_policy
