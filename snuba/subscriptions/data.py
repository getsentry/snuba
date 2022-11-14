from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from concurrent.futures import Future
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
    Tuple,
    Union,
)
from uuid import UUID

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity_subscriptions.entity_subscription import EntitySubscription
from snuba.datasets.entity_subscriptions.validators import InvalidSubscriptionError
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
from snuba.query.query_settings import SubscriptionQuerySettings
from snuba.reader import Result
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
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


@dataclass(frozen=True)
class SubscriptionData:
    """
    Represents the state of a subscription.
    """

    project_id: int
    resolution_sec: int
    time_window_sec: int
    entity_subscription: EntitySubscription
    query: str
    metadata: Mapping[str, Any]

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
                Literal(None, (timestamp - timedelta(seconds=self.time_window_sec))),
            ),
            binary_condition(
                ConditionFunctions.LT,
                Column(None, None, required_timestamp_column),
                Literal(None, timestamp),
            ),
        ]

        new_condition = combine_and_conditions(conditions_to_add)
        condition = query.get_condition()
        if condition:
            new_condition = binary_condition(
                BooleanFunctions.AND, condition, new_condition
            )

        query.set_ast_condition(new_condition)

        if self.entity_subscription.processors:
            for processor in self.entity_subscription.processors:
                processor.process(query, self.metadata)

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
        if self.entity_subscription.validators:
            for validator in self.entity_subscription.validators:
                custom_processing.append(validator.validate)
        custom_processing.append(partial(self.add_conditions, timestamp, offset))

        request = build_request(
            {"query": self.query},
            parse_snql_query,
            SubscriptionQuerySettings,
            schema,
            dataset,
            timer,
            referrer,
            custom_processing,
        )
        return request

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> SubscriptionData:
        entity_subscription = get_entity(entity_key).get_entity_subscription()
        assert entity_subscription is not None

        return SubscriptionData(
            project_id=data["project_id"],
            # Always cast to int in case any old subscriptions are stored as float
            time_window_sec=int(data["time_window"]),
            resolution_sec=int(data["resolution"]),
            query=data["query"],
            entity_subscription=entity_subscription,
            metadata=data,
        )

    def to_dict(self) -> Mapping[str, Any]:
        subscription_data_dict = {
            "project_id": self.project_id,
            "time_window": self.time_window_sec,
            "resolution": self.resolution_sec,
            "query": self.query,
        }
        if self.entity_subscription.processors:
            for processor in self.entity_subscription.processors:
                subscription_data_dict.update(processor.to_dict(self.metadata))
        if self.entity_subscription.validators:
            for validator in self.entity_subscription.validators:
                subscription_data_dict.update(validator.to_dict())
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
