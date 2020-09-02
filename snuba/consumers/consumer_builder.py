from enum import Enum
from typing import Optional, Sequence

from confluent_kafka import KafkaError, KafkaException, Producer

from snuba import environment
from snuba.consumer import ConsumerWorker, StreamingConsumerStrategyFactory
from snuba.consumers.snapshot_worker import SnapshotAwareWorker
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.snapshots import SnapshotId
from snuba.stateful_consumer.control_protocol import TransactionData
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.utils.retries import BasicRetryPolicy, RetryPolicy, constant_delay
from snuba.utils.streams.batching import BatchProcessingStrategyFactory
from snuba.utils.streams.kafka import (
    KafkaConsumer,
    KafkaConsumerWithCommitLog,
    KafkaPayload,
    TransportError,
    build_kafka_consumer_configuration,
)
from snuba.utils.streams.processing import ProcessingStrategyFactory, StreamProcessor
from snuba.utils.streams.types import Topic


StrategyFactoryType = Enum("StrategyFactoryType", ["BATCHING", "STREAMING"])


class ConsumerBuilder:
    """
    Simplifies the initialization of a consumer by merging parameters that
    generally come from the command line with defaults that come from the
    dataset class and defaults that come from the settings file.
    """

    def __init__(
        self,
        storage_key: StorageKey,
        raw_topic: Optional[str],
        replacements_topic: Optional[str],
        max_batch_size: int,
        max_batch_time_ms: int,
        bootstrap_servers: Sequence[str],
        group_id: str,
        commit_log_topic: Optional[str],
        auto_offset_reset: str,
        queued_max_messages_kbytes: int,
        queued_min_messages: int,
        strategy_factory_type: StrategyFactoryType,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        commit_retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        self.storage = get_writable_storage(storage_key)
        self.bootstrap_servers = bootstrap_servers

        stream_loader = self.storage.get_table_writer().get_stream_loader()

        self.raw_topic: Topic
        if raw_topic is not None:
            self.raw_topic = Topic(raw_topic)
        else:
            self.raw_topic = Topic(stream_loader.get_default_topic_spec().topic_name)

        self.replacements_topic: Optional[Topic]
        if replacements_topic is not None:
            self.replacements_topic = Topic(replacements_topic)
        else:
            replacement_topic_spec = stream_loader.get_replacement_topic_spec()
            if replacement_topic_spec is not None:
                self.replacements_topic = Topic(replacement_topic_spec.topic_name)
            else:
                self.replacements_topic = None

        self.commit_log_topic: Optional[Topic]
        if commit_log_topic is not None:
            self.commit_log_topic = Topic(commit_log_topic)
        else:
            commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()
            if commit_log_topic_spec is not None:
                self.commit_log_topic = Topic(commit_log_topic_spec.topic_name)
            else:
                self.commit_log_topic = None

        # XXX: This can result in a producer being built in cases where it's
        # not actually required.
        self.producer = Producer(
            {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "partitioner": "consistent",
                "message.max.bytes": 50000000,  # 50MB, default is 1MB
            }
        )

        self.metrics = MetricsWrapper(
            environment.metrics,
            "consumer",
            tags={"group": group_id, "storage": storage_key.value},
        )

        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.queued_max_messages_kbytes = queued_max_messages_kbytes
        self.queued_min_messages = queued_min_messages
        self.strategy_factory_type = strategy_factory_type
        self.processes = processes
        self.input_block_size = input_block_size
        self.output_block_size = output_block_size

        if (
            self.processes is not None
            and self.strategy_factory_type is not StrategyFactoryType.STREAMING
        ):
            raise ValueError(
                "process count can only be specified when using streaming strategy"
            )

        if commit_retry_policy is None:
            commit_retry_policy = BasicRetryPolicy(
                3,
                constant_delay(1),
                lambda e: isinstance(e, KafkaException)
                and e.args[0].code()
                in (
                    KafkaError.REQUEST_TIMED_OUT,
                    KafkaError.NOT_COORDINATOR_FOR_GROUP,
                    KafkaError._WAIT_COORD,
                ),
            )

        self.__commit_retry_policy = commit_retry_policy

    def __build_consumer(
        self, strategy_factory: ProcessingStrategyFactory[KafkaPayload]
    ) -> StreamProcessor[KafkaPayload]:
        configuration = build_kafka_consumer_configuration(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            queued_max_messages_kbytes=self.queued_max_messages_kbytes,
            queued_min_messages=self.queued_min_messages,
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

        return StreamProcessor(
            consumer,
            self.raw_topic,
            strategy_factory,
            recoverable_errors=[TransportError],
        )

    def __build_batching_strategy_factory(
        self,
    ) -> BatchProcessingStrategyFactory[KafkaPayload]:
        return BatchProcessingStrategyFactory(
            worker=ConsumerWorker(
                storage=self.storage,
                producer=self.producer,
                replacements_topic=self.replacements_topic,
                metrics=self.metrics,
            ),
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms,
            metrics=self.metrics,
        )

    def __build_streaming_strategy_factory(self) -> StreamingConsumerStrategyFactory:
        table_writer = self.storage.get_table_writer()
        stream_loader = table_writer.get_stream_loader()
        return StreamingConsumerStrategyFactory(
            stream_loader.get_pre_filter(),
            stream_loader.get_processor(),
            table_writer.get_writer(
                options={"load_balancing": "in_order", "insert_distributed_sync": 1},
            ),
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms / 1000.0,
            processes=self.processes,
            input_block_size=self.input_block_size,
            output_block_size=self.output_block_size,
            replacements_producer=(
                self.producer if self.replacements_topic is not None else None
            ),
            replacements_topic=self.replacements_topic,
        )

    def build_base_consumer(self) -> StreamProcessor[KafkaPayload]:
        """
        Builds the consumer with the defined processing strategy.
        """
        strategy_factory: ProcessingStrategyFactory[KafkaPayload]
        if self.strategy_factory_type is StrategyFactoryType.BATCHING:
            strategy_factory = self.__build_batching_strategy_factory()
        elif self.strategy_factory_type is StrategyFactoryType.STREAMING:
            strategy_factory = self.__build_streaming_strategy_factory()
        else:
            raise ValueError("unexpected strategy factory type")

        return self.__build_consumer(strategy_factory)

    def build_snapshot_aware_consumer(
        self, snapshot_id: SnapshotId, transaction_data: TransactionData,
    ) -> StreamProcessor[KafkaPayload]:
        """
        Builds the consumer with a ConsumerWorker able to handle snapshots.
        """
        return self.__build_consumer(
            BatchProcessingStrategyFactory(
                worker=SnapshotAwareWorker(
                    storage=self.storage,
                    producer=self.producer,
                    snapshot_id=snapshot_id,
                    transaction_data=transaction_data,
                    metrics=self.metrics,
                    replacements_topic=self.replacements_topic,
                ),
                max_batch_size=self.max_batch_size,
                max_batch_time=self.max_batch_time_ms,
                metrics=self.metrics,
            )
        )
