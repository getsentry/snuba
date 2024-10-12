import logging
import signal
from typing import Any, Mapping, Optional, Sequence

import click
import rapidjson
import sentry_sdk
from arroyo import configure_metrics
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import (
    CommitOffsets,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import Commit, Message, Partition
from attr import dataclass

from snuba import environment, settings
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.cli.lw_delete_batching import BatchStepCustom
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.consumers.consumer_config import resolve_consumer_config
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.web.bulk_delete_query import construct_or_conditions, construct_query
from snuba.web.delete_query import TooManyOngoingMutationsError, _execute_query

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--consumer-group",
    help="Consumer group use for consuming the deletion topic.",
    default="lw-deletions-consumer",
    required=True,
)
@click.option(
    "--bootstrap-servers",
    multiple=True,
    help="Kafka bootstrap server to use for consuming.",
)
@click.option("--storage-name", help="Storage name to consume from", required=True)
@click.option(
    "--auto-offset-reset",
    default="earliest",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option(
    "--no-strict-offset-reset",
    is_flag=True,
    help="Forces the kafka consumer auto offset reset.",
)
@click.option(
    "--queued-max-messages-kbytes",
    default=settings.DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    type=int,
    help="Maximum number of kilobytes per topic+partition in the local consumer queue.",
)
@click.option(
    "--queued-min-messages",
    default=settings.DEFAULT_QUEUED_MIN_MESSAGES,
    type=int,
    help="Minimum number of messages per topic+partition the local consumer queue should contain before messages are sent to kafka.",
)
@click.option("--log-level", help="Logging level to use.")
def lw_deletions_consumer(
    *,
    consumer_group: str,
    bootstrap_servers: Sequence[str],
    storage_name: str,
    max_batch_size: int = 6,
    max_batch_time_ms: int = 1000,
    max_insert_batch_size: int = 0,
    max_insert_batch_time_ms: int = 0,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    log_level: str,
) -> None:
    setup_logging(log_level)
    setup_sentry()

    logger.info("Consumer Starting")

    sentry_sdk.set_tag("storage", storage_name)
    shutdown_requested = False
    consumer: Optional[StreamProcessor[KafkaPayload]] = None

    def handler(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True

        if consumer is not None:
            consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    while not shutdown_requested:
        metrics_tags = {
            "consumer_group": consumer_group,
            "storage": storage_name,
        }
        metrics = MetricsWrapper(
            environment.metrics, "lw_deletions_consumer", tags=metrics_tags
        )
        configure_metrics(StreamMetricsAdapter(metrics), force=True)
        consumer_config = resolve_consumer_config(
            storage_names=[storage_name],
            raw_topic=SnubaTopic.LW_DELETIONS.value,
            commit_log_topic=None,
            replacements_topic=None,
            bootstrap_servers=bootstrap_servers,
            commit_log_bootstrap_servers=[],
            replacement_bootstrap_servers=[],
            slice_id=None,
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            group_instance_id=consumer_group,
        )

        consumer_builder = ConsumerBuilder(
            consumer_config=consumer_config,
            kafka_params=KafkaParameters(
                group_id=consumer_group,
                auto_offset_reset=auto_offset_reset,
                strict_offset_reset=not no_strict_offset_reset,
                queued_max_messages_kbytes=queued_max_messages_kbytes,
                queued_min_messages=queued_min_messages,
            ),
            processing_params=ProcessingParameters(None, None, None),
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            max_insert_batch_size=max_insert_batch_size,
            max_insert_batch_time_ms=max_insert_batch_time_ms,
            metrics=metrics,
            slice_id=None,
            join_timeout=None,
            enforce_schema=False,
            metrics_tags=metrics_tags,
        )

        storage = get_writable_storage(StorageKey(storage_name))
        strategy_factory = ConsumerStrategyFactory(
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            storage=storage,
            formatter=SearchIssuesFormatter(),
        )

        consumer = consumer_builder.build_consumer(strategy_factory)

        consumer.run()
        consumer_builder.flush()


@dataclass
class AndCondition:
    project_id: int
    group_ids: Sequence[int] = []

    def add_group_ids(self, group_ids: Sequence[int]) -> None:
        [self.group_ids.append(g_id) for g_id in group_ids]


class Formatter:
    def format(self) -> str:
        pass


class SearchIssuesFormatter(Formatter):
    def format(self, messages: Sequence[Message]) -> Sequence[Mapping]:
        mapping: Mapping[int, AndCondition] = {}
        for message in messages:
            project_id = message["conditions"]["project_id"][0]
            group_ids = message["conditions"]["group_id"]
            and_condition = mapping.get(project_id, AndCondition(project_id, []))
            and_condition.add_group_ids(group_ids)
            mapping[project_id] = and_condition

        and_conditions = []
        for and_condition in mapping.values():
            and_conditions.append(
                {
                    "project_id": [and_condition.project_id],
                    "group_id": and_condition.group_ids,
                }
            )
        return and_conditions


class FormatQuery(ProcessingStrategy[KafkaPayload]):
    def __init__(
        self,
        next_step: ProcessingStrategy[KafkaPayload],
        storage: WritableTableStorage,
        formatter: Formatter,
    ) -> None:
        self.__next_step = next_step
        self.__storage = storage
        self.__tables = storage.get_deletion_settings().tables
        self.__formatter: Formatter = formatter

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        decode_messages = [
            rapidjson.loads(m.payload.value) for m in message.value.payload
        ]
        conditions = self.__formatter.format(decode_messages)

        try:
            self._execute_delete(conditions)
        except TooManyOngoingMutationsError:
            raise MessageRejected

        self.__next_step.submit(message)

    def _get_attribute_info(self) -> AttributionInfo:
        return AttributionInfo(
            app_id=AppID("lw-deletes"),
            tenant_ids={},
            referrer="lw-deletes",
            team=None,
            feature=None,
            parent_api=None,
        )

    def _execute_delete(self, conditions) -> None:
        query_settings = HTTPQuerySettings()
        for table in self.__tables:
            query = construct_query(
                self.__storage, table, construct_or_conditions(conditions)
            )
            _execute_query(
                query=query,
                storage=self.__storage,
                table=table,
                attribution_info=self._get_attribute_info(),
                query_settings=query_settings,
            )

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)


class ConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    """
    The factory manages the lifecycle of the `ProcessingStrategy`.
    A strategy is created every time new partitions are assigned to the
    consumer, while it is destroyed when partitions are revoked or the
    consumer is closed
    """

    def __init__(
        self,
        max_batch_size: int,
        max_batch_time_ms: int,
        storage: WritableTableStorage,
        formatter: Formatter,
    ) -> None:
        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.storage = storage
        self.formatter = formatter

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        batch_step = BatchStepCustom(
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time_ms,
            next_step=FormatQuery(CommitOffsets(commit), self.storage, self.formatter),
        )
        return batch_step
