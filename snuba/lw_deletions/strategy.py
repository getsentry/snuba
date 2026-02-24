import hashlib
import json
import logging
import time
import typing
from datetime import datetime, timedelta
from typing import List, Mapping, Optional, Sequence, TypeVar

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
from snuba.cleanup import get_active_partitions
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.query import Query
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.batching import BatchStepCustom, ValuesBatch
from snuba.lw_deletions.formatters import Formatter
from snuba.lw_deletions.types import ConditionsBag
from snuba.query.allocation_policies import AllocationPolicyViolations
from snuba.query.conditions import combine_and_conditions
from snuba.query.dsl import column, equals, literal
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.query_settings import HTTPQuerySettings
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state import get_int_config, get_str_config
from snuba.utils.metrics import MetricsBackend
from snuba.web import QueryException
from snuba.web.bulk_delete_query import construct_or_conditions, construct_query
from snuba.web.constants import LW_DELETE_NON_RETRYABLE_CLICKHOUSE_ERROR_CODES
from snuba.web.delete_query import (
    TooManyOngoingMutationsError,
    _execute_query,
    _num_ongoing_mutations,
)

TPayload = TypeVar("TPayload")

logger = logging.getLogger(__name__)


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
        deletion_settings = storage.get_deletion_settings()
        self.__tables = deletion_settings.tables
        self.__partition_column = deletion_settings.partition_column
        self.__formatter: Formatter = formatter
        self.__metrics = metrics
        self.__last_ongoing_mutations_check: Optional[float] = None

    def poll(self) -> None:
        self.__next_step.poll()

    # TODO: _is_execute_enabled is for EAP testing purposes, this should be removed after launch
    def _is_execute_enabled(self, conditions: Sequence[ConditionsBag]) -> bool:
        if self.__storage.get_storage_key() != StorageKey.EAP_ITEMS:
            return True

        query_org_ids: list[int] = [
            int(org_id)
            for cond in conditions
            for org_id in cond.column_conditions.get("organization_id", [])
        ]
        assert len(query_org_ids) > 0, "No organization IDs found in conditions"
        # allowlist not being set implicitly allows all
        if get_str_config("org_ids_delete_allowlist", "") == "":
            return True
        else:
            str_config = get_str_config("org_ids_delete_allowlist", "")
            assert str_config
            org_ids_delete_allowlist = set([int(org_id) for org_id in str_config.split(",")])
            logger.info(f"query conditions: {conditions}, allowlist: {org_ids_delete_allowlist}")
            return org_ids_delete_allowlist.issuperset(query_org_ids)

    def submit(self, message: Message[ValuesBatch[KafkaPayload]]) -> None:
        decode_messages = [rapidjson.loads(m.payload.value) for m in message.value.payload]
        conditions = self.__formatter.format(decode_messages)

        try:
            if self._is_execute_enabled(conditions):
                self._execute_delete(conditions)
            else:
                self.__metrics.increment("delete_skipped")
        except TooManyOngoingMutationsError:
            # backpressure is applied while we wait for the
            # currently ongoing mutations to finish
            self.__metrics.increment("too_many_ongoing_mutations")
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

    def _conditions_hash(self, conditions: Sequence[ConditionsBag]) -> str:
        parts = []
        for c in conditions:
            parts.append(json.dumps(dict(c.column_conditions), sort_keys=True))
            if c.attribute_conditions:
                parts.append(
                    json.dumps(
                        {
                            "item_type": c.attribute_conditions.item_type,
                            "attributes": {
                                k: [v[0].type, v[0].name, v[1]]
                                for k, v in c.attribute_conditions.attributes.items()
                            },
                        },
                        sort_keys=True,
                    )
                )
        parts.sort()
        return hashlib.md5("|".join(parts).encode()).hexdigest()[:16]

    def _get_partition_dates(self, table: str) -> List[str]:
        cluster = self.__storage.get_cluster()
        database = cluster.get_database()
        connection = cluster.get_query_connection(ClickhouseClientSettings.QUERY)
        parts = get_active_partitions(connection, self.__storage, database, table)
        now = datetime.now()
        min_date = (now - timedelta(days=365)).date()
        max_date = (now + timedelta(days=7)).date()
        all_dates = sorted({part.date.strftime("%Y-%m-%d") for part in parts})
        valid_dates = [
            d for d in all_dates if min_date <= datetime.strptime(d, "%Y-%m-%d").date() <= max_date
        ]
        skipped = len(all_dates) - len(valid_dates)
        if skipped:
            self.__metrics.increment(
                "partition_date_filtered", value=skipped, tags={"table": table}
            )
        return valid_dates

    def _execute_delete(self, conditions: Sequence[ConditionsBag]) -> None:
        self._check_ongoing_mutations()
        query_settings = HTTPQuerySettings()
        # starting in 24.4 the default is 2
        lw_sync = get_int_config("lightweight_deletes_sync")
        if lw_sync is not None:
            query_settings.set_clickhouse_settings({"lightweight_deletes_sync": lw_sync})

        split_enabled = bool(
            self.__partition_column
            and get_int_config(f"lw_deletes_split_by_partition_{self.__storage_name}", default=0)
        )

        for table in self.__tables:
            where_clause = construct_or_conditions(self.__storage, conditions)
            if split_enabled:
                self._execute_delete_by_partition(table, where_clause, query_settings, conditions)
            else:
                query = construct_query(self.__storage, table, where_clause)
                self._execute_single_delete(table, query, query_settings)

    def _execute_delete_by_partition(
        self,
        table: str,
        where_clause: Expression,
        query_settings: HTTPQuerySettings,
        conditions: Sequence[ConditionsBag],
    ) -> None:
        partition_dates = self._get_partition_dates(table)
        if not partition_dates:
            logger.warning(
                "No partitions found for table %s, falling back to un-split delete",
                table,
            )
            query = construct_query(self.__storage, table, where_clause)
            self._execute_single_delete(table, query, query_settings)
            return

        cond_hash = self._conditions_hash(conditions)
        tracking_key = f"lw_delete_partitions:{self.__storage_name}:{cond_hash}"
        redis_client = get_redis_client(RedisClientKey.CONFIG)
        ttl = settings.LW_DELETES_PARTITION_TRACKING_TTL

        for partition_date in partition_dates:
            member = f"{table}:{partition_date}"
            if redis_client.sismember(tracking_key, member):
                self.__metrics.increment(
                    "partition_delete_skipped",
                    tags={"table": table, "partition_date": partition_date},
                )
                logger.info(
                    "Skipping already-tracked partition %s for table %s",
                    partition_date,
                    table,
                )
                continue

            self._check_ongoing_mutations(skip_throttle=True)
            partition_condition = equals(
                FunctionCall(None, "toMonday", (column(self.__partition_column),)),  # type: ignore[arg-type]
                literal(partition_date),
            )
            partition_where = combine_and_conditions([where_clause, partition_condition])
            query = construct_query(self.__storage, table, partition_where)
            self._execute_single_delete(table, query, query_settings, partition_date=partition_date)
            self.__metrics.increment(
                "partition_delete_executed",
                tags={"table": table, "partition_date": partition_date},
            )
            redis_client.sadd(tracking_key, member)

        redis_client.expire(tracking_key, ttl)

    def _execute_single_delete(
        self,
        table: str,
        query: Query,
        query_settings: HTTPQuerySettings,
        partition_date: Optional[str] = None,
    ) -> None:
        tags = {"table": table}
        if partition_date:
            tags["partition_date"] = partition_date
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
                tags=tags,
            )
        except QueryException as exc:
            self.__metrics.increment("execute_delete_query_failed", tags=tags)
            cause = exc.__cause__
            if isinstance(cause, ClickhouseError):
                if cause.code in LW_DELETE_NON_RETRYABLE_CLICKHOUSE_ERROR_CODES:
                    logger.exception("Error running delete query %r", exc)
                else:
                    raise LWDeleteQueryException(exc.message)

    def _check_ongoing_mutations(self, skip_throttle: bool = False) -> None:
        now = time.time()
        if (
            not skip_throttle
            and self.__last_ongoing_mutations_check is not None
            and now - self.__last_ongoing_mutations_check < 1.0
        ):
            raise TooManyOngoingMutationsError(
                "ongoing mutations check is throttled to once per second"
            )
        start = time.time()
        ongoing_mutations = _num_ongoing_mutations(self.__storage.get_cluster(), self.__tables)
        self.__last_ongoing_mutations_check = time.time()
        max_ongoing_mutations = typing.cast(
            int,
            get_int_config(
                "max_ongoing_mutations_for_delete",
                default=settings.MAX_ONGOING_MUTATIONS_FOR_DELETE,
            ),
        )
        self.__metrics.timing("ongoing_mutations_query_ms", (time.time() - start) * 1000)
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
