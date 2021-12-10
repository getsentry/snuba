from __future__ import annotations

import logging
from abc import ABC, abstractclassmethod, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import (
    Any,
    Iterator,
    Mapping,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    Union,
)
from uuid import UUID

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.request import Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.subscriptions.entity_subscription import (
    ENTITY_KEY_TO_SUBSCRIPTION_MAPPER,
    EntitySubscription,
    InvalidSubscriptionError,
    SubscriptionType,
)
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer

SUBSCRIPTION_REFERRER = "subscription"

logger = logging.getLogger("snuba.subscriptions")


PartitionId = NewType("PartitionId", int)


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


# This is a workaround for a mypy bug, found here: https://github.com/python/mypy/issues/5374
@dataclass(frozen=True)
class _SubscriptionData:
    project_id: int
    resolution: timedelta
    time_window: timedelta
    entity_subscription: EntitySubscription


class SubscriptionData(ABC, _SubscriptionData):
    """
    Represents the state of a subscription.
    """

    TYPE_FIELD = "type"

    def __post_init__(self) -> None:
        if self.time_window < timedelta(minutes=1):
            raise InvalidSubscriptionError(
                "Time window must be greater than or equal to 1 minute"
            )
        elif self.time_window > timedelta(hours=24):
            raise InvalidSubscriptionError(
                "Time window must be less than or equal to 24 hours"
            )

        if self.resolution < timedelta(minutes=1):
            raise InvalidSubscriptionError(
                "Resolution must be greater than or equal to 1 minute"
            )

        if self.resolution.microseconds > 0:
            raise InvalidSubscriptionError("Resolution does not support microseconds")

    @abstractmethod
    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> Request:
        raise NotImplementedError

    @abstractclassmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> SubscriptionData:
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class SnQLSubscriptionData(SubscriptionData):
    query: str

    def add_conditions(
        self,
        timestamp: datetime,
        offset: Optional[int],
        query: Union[CompositeQuery[Entity], Query],
    ) -> None:
        # TODO: Support composite queries with multiple entities.
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, Entity):
            raise InvalidSubscriptionError("Only simple queries are supported")
        entity = get_entity(from_clause.key)
        required_timestamp_column = entity.required_time_column
        if required_timestamp_column is None:
            raise InvalidSubscriptionError(
                "Entity must have a timestamp column for subscriptions"
            )

        conditions_to_add: Sequence[Expression] = [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "project_id"),
                Literal(None, self.project_id),
            ),
            binary_condition(
                ConditionFunctions.GTE,
                Column(None, None, required_timestamp_column),
                Literal(None, (timestamp - self.time_window)),
            ),
            binary_condition(
                ConditionFunctions.LT,
                Column(None, None, required_timestamp_column),
                Literal(None, timestamp),
            ),
            *self.entity_subscription.get_entity_subscription_conditions_for_snql(
                offset
            ),
        ]

        new_condition = combine_and_conditions(conditions_to_add)
        condition = query.get_condition()
        if condition:
            new_condition = binary_condition(
                BooleanFunctions.AND, condition, new_condition
            )

        query.set_ast_condition(new_condition)

    def validate_subscription(
        self, query: Union[CompositeQuery[Entity], Query]
    ) -> None:
        self.entity_subscription.validate_query(query)

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> Request:
        schema = RequestSchema.build(SubscriptionRequestSettings)

        request = build_request(
            {"query": self.query},
            parse_snql_query,
            SubscriptionRequestSettings,
            schema,
            dataset,
            timer,
            referrer,
            [
                self.validate_subscription,
                partial(self.add_conditions, timestamp, offset),
            ],
        )
        return request

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> SnQLSubscriptionData:
        entity_subscription = ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[entity_key](
            data_dict=data
        )

        return SnQLSubscriptionData(
            project_id=data["project_id"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
            query=data["query"],
            entity_subscription=entity_subscription,
        )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            self.TYPE_FIELD: SubscriptionType.SNQL.value,
            "project_id": self.project_id,
            "time_window": int(self.time_window.total_seconds()),
            "resolution": int(self.resolution.total_seconds()),
            "query": self.query,
            **self.entity_subscription.to_dict(),
        }


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData


class SubscriptionWithTick(NamedTuple):
    entity: EntityKey
    subscription: Subscription
    tick: Tick


@dataclass(frozen=True)
class ScheduledSubscriptionTask:
    """
    A scheduled task represents a unit of work (a task) that is intended to
    be executed at (or around) a specific time.
    """

    # The time that this task was scheduled to execute.
    timestamp: datetime

    # The task that should be executed.
    task: SubscriptionWithTick


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
