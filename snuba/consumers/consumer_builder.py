from typing import Optional, Sequence

from confluent_kafka import KafkaError, KafkaException, Producer

from snuba import settings, util
from snuba.consumer import ConsumerWorker
from snuba.consumers.snapshot_worker import SnapshotAwareWorker
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.snapshots import SnapshotId
from snuba.stateful_consumer.control_protocol import TransactionData
from snuba.utils.codecs import PassthroughCodec
from snuba.utils.retries import BasicRetryPolicy, RetryPolicy, constant_delay
from snuba.utils.streams.batching import BatchingConsumer
from snuba.utils.streams.kafka import (
    KafkaConsumer,
    KafkaConsumerWithCommitLog,
    KafkaPayload,
    TransportError,
    build_kafka_consumer_configuration,
)
from snuba.utils.streams.types import Topic


class ConsumerBuilder:
    """
    Simplifies the initialization of a batching consumer by merging
    parameters that generally come from the command line with defaults
    that come from the dataset class and defaults that come from the
    settings file.
    """

    def __init__(
        self,
        dataset_name: str,
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
        rapid_json_deserialize: bool,
        rapid_json_serialize: bool,
        commit_retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        self.dataset = get_dataset(dataset_name)
        self.dataset_name = dataset_name
        if not bootstrap_servers:
            self.bootstrap_servers = settings.DEFAULT_DATASET_BROKERS.get(
                dataset_name, settings.DEFAULT_BROKERS,
            )
        else:
            self.bootstrap_servers = bootstrap_servers

        stream_loader = enforce_table_writer(self.dataset).get_stream_loader()

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

        self.metrics = util.create_metrics(
            "snuba.consumer", tags={"group": group_id, "dataset": self.dataset_name},
        )

        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.queued_max_messages_kbytes = queued_max_messages_kbytes
        self.queued_min_messages = queued_min_messages

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
        self.__rapid_json_deserialize = rapid_json_deserialize
        self.__rapid_json_serialize = rapid_json_serialize

    def __build_consumer(
        self, worker: ConsumerWorker
    ) -> BatchingConsumer[KafkaPayload]:
        configuration = build_kafka_consumer_configuration(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            queued_max_messages_kbytes=self.queued_max_messages_kbytes,
            queued_min_messages=self.queued_min_messages,
        )

        codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
        if self.commit_log_topic is None:
            consumer = KafkaConsumer(
                configuration,
                codec=codec,
                commit_retry_policy=self.__commit_retry_policy,
            )
        else:
            consumer = KafkaConsumerWithCommitLog(
                configuration,
                codec=codec,
                producer=self.producer,
                commit_log_topic=self.commit_log_topic,
                commit_retry_policy=self.__commit_retry_policy,
            )

        return BatchingConsumer(
            consumer,
            self.raw_topic,
            worker=worker,
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms,
            metrics=self.metrics,
            recoverable_errors=[TransportError],
        )

    def build_base_consumer(self) -> BatchingConsumer[KafkaPayload]:
        """
        Builds the consumer with a ConsumerWorker.
        """
        return self.__build_consumer(
            ConsumerWorker(
                self.dataset,
                producer=self.producer,
                replacements_topic=self.replacements_topic,
                metrics=self.metrics,
                rapid_json_deserialize=self.__rapid_json_deserialize,
                rapid_json_serialize=self.__rapid_json_serialize,
            )
        )

    def build_snapshot_aware_consumer(
        self, snapshot_id: SnapshotId, transaction_data: TransactionData,
    ) -> BatchingConsumer[KafkaPayload]:
        """
        Builds the consumer with a ConsumerWorker able to handle snapshots.
        """
        worker = SnapshotAwareWorker(
            dataset=self.dataset,
            producer=self.producer,
            snapshot_id=snapshot_id,
            transaction_data=transaction_data,
            metrics=self.metrics,
            replacements_topic=self.replacements_topic,
        )
        return self.__build_consumer(worker)
