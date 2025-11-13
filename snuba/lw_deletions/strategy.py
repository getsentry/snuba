import time
import typing
from typing import Mapping, Optional, Sequence, TypeVar

import rapidjson
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import (
    CommitOffsets,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import BaseValue, Commit, Message, Partition

from snuba import settings
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.storage import WritableTableStorage
from snuba.lw_deletions.batching import BatchStepCustom, ValuesBatch
from snuba.lw_deletions.formatters import Formatter
from snuba.query.allocation_policies import AllocationPolicyViolations
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import get_int_config
from snuba.utils.metrics import MetricsBackend
from snuba.web import QueryException
from snuba.web.bulk_delete_query import construct_or_conditions, construct_query
from snuba.web.constants import LW_DELETE_NON_RETRYABLE_CLICKHOUSE_ERROR_CODES
from snuba.web.delete_query import (
    ConditionsType,
    TooManyOngoingMutationsError,
    _execute_query,
    _num_ongoing_mutations,
)

TPayload = TypeVar("TPayload")

import logging

logger = logging.Logger(__name__)


class LWDeleteQueryException(Exception):
    pass


class FormatQuery(ProcessingStrategy[ValuesBatch[KafkaPayload]]):
    def __init__(
        self,
        next_step: ProcessingStrategy[ValuesBatch[KafkaPayload]],
        storage: WritableTableStorage,
        formatter: Formatter,
        metrics: MetricsBackend,
    ) -> None:
        self.__next_step = next_step
        self.__storage = storage
        self.__storage_name = storage.get_storage_key().value
        self.__cluster_name = self.__storage.get_cluster().get_clickhouse_cluster_name()
        self.__tables = storage.get_deletion_settings().tables
        self.__formatter: Formatter = formatter
        self.__metrics = metrics

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[ValuesBatch[KafkaPayload]]) -> None:
        decode_messages = [rapidjson.loads(m.payload.value) for m in message.value.payload]
        conditions = self.__formatter.format(decode_messages)

        try:
            self._execute_delete(conditions)
        except TooManyOngoingMutationsError as err:
            # backpressure is applied while we wait for the
            # currently ongoing mutations to finish
            self.__metrics.increment("too_many_ongoing_mutations")
            logger.warning(str(err), exc_info=True)
            raise MessageRejected
        except QueryException as err:
            cause = err.__cause__
            if isinstance(cause, AllocationPolicyViolations):
                self.__metrics.increment("allocation_policy_violation")
                raise MessageRejected

        self.__next_step.submit(message)

    def _get_attribute_info(self) -> AttributionInfo:
        return AttributionInfo(
            app_id=AppID("lw-deletes"),
            # concurrent allocation policies requires project or org id
            tenant_ids={"project_id": 1},
            referrer="lw-deletes",
            team=None,
            feature=None,
            parent_api=None,
        )

    def _execute_delete(self, conditions: Sequence[ConditionsType]) -> None:
        self._check_ongoing_mutations()
        query_settings = HTTPQuerySettings()
        # starting in 24.4 the default is 2
        lw_sync = get_int_config("lightweight_deletes_sync")
        if lw_sync:
            query_settings.set_clickhouse_settings({"lightweight_deletes_sync": lw_sync})
        for table in self.__tables:
            query = construct_query(self.__storage, table, construct_or_conditions(conditions))
            start = time.time()
            try:
                _execute_query(
                    query=query,
                    storage=self.__storage,
                    cluster_name=self.__cluster_name,
                    table=table,
                    attribution_info=self._get_attribute_info(),
                    query_settings=query_settings,
                )
                self.__metrics.timing(
                    "execute_delete_query_ms",
                    (time.time() - start) * 1000,
                    tags={"table": table},
                )
            except QueryException as exc:
                self.__metrics.increment("execute_delete_query_failed", tags={"table": table})
                cause = exc.__cause__
                if isinstance(cause, ClickhouseError):
                    if cause.code in LW_DELETE_NON_RETRYABLE_CLICKHOUSE_ERROR_CODES:
                        logger.exception("Error running delete query %r", exc)
                    else:
                        raise LWDeleteQueryException(exc.message)

    def _check_ongoing_mutations(self) -> None:
        start = time.time()
        ongoing_mutations = _num_ongoing_mutations(self.__storage.get_cluster(), self.__tables)
        max_ongoing_mutations = typing.cast(
            int,
            get_int_config(
                "max_ongoing_mutatations_for_delete",
                default=settings.MAX_ONGOING_MUTATIONS_FOR_DELETE,
            ),
        )
        self.__metrics.timing("ongoing_mutations_query_ms", (time.time() - start) * 1000)
        max_ongoing_mutations = int(settings.MAX_ONGOING_MUTATIONS_FOR_DELETE)
        if ongoing_mutations > max_ongoing_mutations:

            raise TooManyOngoingMutationsError(
                f"{ongoing_mutations} mutations for {self.__tables} table(s) is above max ongoing mutations: {max_ongoing_mutations} "
            )

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)


def increment_by(message: BaseValue[KafkaPayload]) -> int:
    rows_to_delete = rapidjson.loads(message.payload.value)["rows_to_delete"]
    assert isinstance(rows_to_delete, int)
    return rows_to_delete


class LWDeletionsConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
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
        metrics: MetricsBackend,
    ) -> None:
        self.max_batch_size = max_batch_size
        self.max_batch_time_ms = max_batch_time_ms
        self.storage = storage
        self.formatter = formatter
        self.metrics = metrics

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        batch_step = BatchStepCustom(
            max_batch_size=self.max_batch_size,
            max_batch_time=(self.max_batch_time_ms / 1000),
            next_step=FormatQuery(
                CommitOffsets(commit), self.storage, self.formatter, self.metrics
            ),
            increment_by=increment_by,
        )
        return batch_step
