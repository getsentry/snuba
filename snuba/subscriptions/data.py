from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import (
    Any,
    Generic,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    NewType,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from uuid import UUID

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.validators import InvalidSubscriptionError
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity as EntityDS
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import SubscriptionQuerySettings
from snuba.reader import Result
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer

SUBSCRIPTION_REFERRER = "subscription"

# These are subscription payload keys which need to be set as attributes in SubscriptionData.
SUBSCRIPTION_DATA_PAYLOAD_KEYS = {
    "project_id",
    "time_window",
    "resolution",
    "query",
    "tenant_ids",
}

logger = logging.getLogger("snuba.subscriptions")

PartitionId = NewType("PartitionId", int)


class SubscriptionType(Enum):
    SNQL = "snql"
    RPC = "rpc"


@dataclass(frozen=True)
class SubscriptionIdentifier:
    partition: PartitionId
    uuid: UUID

    def __str__(self) -> str:
        return f"{self.partition}/{self.uuid.hex}"

    @classmethod
    def from_string(cls, value: str) -> SubscriptionIdentifier:
        partition, uuid = value.split("/")
        return cls(PartitionId(int(partition)), UUID(uuid))


TRequest = TypeVar("TRequest")


@dataclass(frozen=True, kw_only=True)
class SubscriptionData(ABC, Generic[TRequest]):
    project_id: int
    resolution_sec: int
    time_window_sec: int
    entity: Entity
    metadata: Mapping[str, Any]
    tenant_ids: Mapping[str, Any] = field(default_factory=lambda: dict())

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> TRequest:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> SubscriptionData:
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, kw_only=True)
class RPCSubscriptionData(SubscriptionData[TraceItemTableRequest]):
    """
    Represents the state of an RPC subscription.
    """

    table_request: str

    def validate(self) -> None:
        # TODO: Validate data
        pass

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> TraceItemTableRequest:

        # Make table request
        table_request = TraceItemTableRequest()
        table_request.ParseFromString(self.table_request.encode("utf-8"))
        # TODO: Add time conditions etc

        # Add time conditions
        start_time_proto = Timestamp()
        start_time_proto.FromDatetime(timestamp - timedelta(self.time_window_sec))
        end_time_proto = Timestamp()
        end_time_proto.FromDatetime(timestamp)
        table_request.meta.start_timestamp.CopyFrom(start_time_proto)
        table_request.meta.end_timestamp.CopyFrom(end_time_proto)

        return table_request

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> RPCSubscriptionData:
        entity: Entity = get_entity(entity_key)

        metadata = {}
        for key in data.keys():
            if key not in SUBSCRIPTION_DATA_PAYLOAD_KEYS:
                metadata[key] = data[key]

        return RPCSubscriptionData(
            project_id=data["project_id"],
            time_window_sec=int(data["time_window"]),
            resolution_sec=int(data["resolution"]),
            table_request=data["table_request"],
            entity=entity,
            metadata=metadata,
            tenant_ids=data.get("tenant_ids", dict()),
        )

    def to_dict(self) -> Mapping[str, Any]:
        subscription_data_dict = {
            "project_id": self.project_id,
            "time_window": self.time_window_sec,
            "resolution": self.resolution_sec,
            "table_request": self.table_request,
            "subscription_type": SubscriptionType.RPC,
        }

        subscription_processors = self.entity.get_subscription_processors()
        if subscription_processors:
            for processor in subscription_processors:
                subscription_data_dict.update(processor.to_dict(self.metadata))
        return subscription_data_dict


@dataclass(frozen=True, kw_only=True)
class SnQLSubscriptionData(SubscriptionData[Request]):
    """
    Represents the state of a SnQL subscription.
    """

    query: str

    def add_conditions(
        self,
        timestamp: datetime,
        offset: Optional[int],
        query: Union[CompositeQuery[EntityDS], Query],
    ) -> None:
        added_timestamp_column = False
        from_clause = query.get_from_clause()
        entities: List[Tuple[Optional[str], Entity]] = []
        if isinstance(from_clause, JoinClause):
            for alias, node in from_clause.get_alias_node_map().items():
                assert isinstance(node.data_source, EntityDS), node.data_source
                entities.append((alias, get_entity(node.data_source.key)))
        elif isinstance(from_clause, EntityDS):
            entities = [(None, get_entity(from_clause.key))]
        else:
            raise InvalidSubscriptionError(
                "Only simple queries and join queries are supported"
            )
        for entity_alias, entity in entities:
            conditions_to_add: List[Expression] = [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, entity_alias, "project_id"),
                    Literal(None, self.project_id),
                ),
            ]

            required_timestamp_column = entity.required_time_column
            if required_timestamp_column is not None:
                added_timestamp_column = True
                conditions_to_add.extend(
                    [
                        binary_condition(
                            ConditionFunctions.GTE,
                            Column(None, entity_alias, required_timestamp_column),
                            Literal(
                                None,
                                (timestamp - timedelta(seconds=self.time_window_sec)),
                            ),
                        ),
                        binary_condition(
                            ConditionFunctions.LT,
                            Column(None, entity_alias, required_timestamp_column),
                            Literal(None, timestamp),
                        ),
                    ]
                )

            new_condition = combine_and_conditions(conditions_to_add)
            condition = query.get_condition()
            if condition:
                new_condition = binary_condition(
                    BooleanFunctions.AND, condition, new_condition
                )

            query.set_ast_condition(new_condition)

            subscription_processors = self.entity.get_subscription_processors()
            if subscription_processors:
                for processor in subscription_processors:
                    processor.process(query, self.metadata, offset)

        if not added_timestamp_column:
            raise InvalidSubscriptionError(
                "At least one Entity must have a timestamp column for subscriptions"
            )

    def validate(self) -> None:
        if self.time_window_sec < 60:
            raise InvalidSubscriptionError(
                "Time window must be greater than or equal to 1 minute"
            )
        elif self.time_window_sec > 60 * 60 * 24:
            raise InvalidSubscriptionError(
                "Time window must be less than or equal to 24 hours"
            )

        if self.resolution_sec < 60:
            raise InvalidSubscriptionError(
                "Resolution must be greater than or equal to 1 minute"
            )

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> Request:
        schema = RequestSchema.build(SubscriptionQuerySettings)

        custom_processing = []
        subscription_validators = self.entity.get_subscription_validators()
        if subscription_validators:
            for validator in subscription_validators:
                custom_processing.append(validator.validate)
        custom_processing.append(partial(self.add_conditions, timestamp, offset))

        tenant_ids = {**self.tenant_ids}
        tenant_ids["referrer"] = referrer
        if "organization_id" not in tenant_ids:
            # TODO: Subscriptions queries should have an org ID
            tenant_ids["organization_id"] = 1

        request = build_request(
            {
                "query": self.query,
                "tenant_ids": tenant_ids,
            },
            parse_snql_query,
            SubscriptionQuerySettings,
            schema,
            dataset,
            timer,
            referrer,
            # subscriptions are tied to entities, these validators are going to run on the entity
            # anyways so it's okay that the post-processing is done without type safety
            custom_processing,  # type: ignore
        )
        return request

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> SubscriptionData:
        entity: Entity = get_entity(entity_key)

        metadata = {}
        for key in data.keys():
            if key not in SUBSCRIPTION_DATA_PAYLOAD_KEYS:
                metadata[key] = data[key]

        return SnQLSubscriptionData(
            project_id=data["project_id"],
            # Always cast to int in case any old subscriptions are stored as float
            time_window_sec=int(data["time_window"]),
            resolution_sec=int(data["resolution"]),
            query=data["query"],
            entity=entity,
            metadata=metadata,
            tenant_ids=data.get("tenant_ids", dict()),
        )

    def to_dict(self) -> Mapping[str, Any]:
        subscription_data_dict = {
            "project_id": self.project_id,
            "time_window": self.time_window_sec,
            "resolution": self.resolution_sec,
            "query": self.query,
            "subscription_type": SubscriptionType.SNQL,
        }

        subscription_processors = self.entity.get_subscription_processors()
        if subscription_processors:
            for processor in subscription_processors:
                subscription_data_dict.update(processor.to_dict(self.metadata))
        return subscription_data_dict


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData


class SubscriptionWithMetadata(NamedTuple):
    entity: EntityKey
    subscription: Subscription
    tick_upper_offset: int


@dataclass(frozen=True)
class ScheduledSubscriptionTask:
    """
    A scheduled task represents a unit of work (a task) that is intended to
    be executed at (or around) a specific time.
    """

    # The time that this task was scheduled to execute.
    timestamp: datetime

    # The task that should be executed.
    task: SubscriptionWithMetadata


class SubscriptionScheduler(ABC):
    """
    The scheduler maintains the scheduling state for subscription tasks and
    provides the ability to query the schedule to find tasks that should be
    scheduled between the time interval of a tick.
    """

    @abstractmethod
    def find(self, tick: Tick) -> Iterator[ScheduledSubscriptionTask]:
        """
        Find all of the tasks that were scheduled to be executed between the
        lower bound (exclusive) and upper bound (inclusive) of the tick's
        time interval. The tasks returned should be ordered by timestamp in
        ascending order.
        """
        raise NotImplementedError


class SubscriptionTaskResultFuture(NamedTuple):
    task: ScheduledSubscriptionTask
    future: Future[Tuple[Request, Result]]


class SubscriptionTaskResult(NamedTuple):
    task: ScheduledSubscriptionTask
    result: Tuple[Request, Result]
