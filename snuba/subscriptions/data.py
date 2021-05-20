from __future__ import annotations

from abc import ABC, abstractclassmethod, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import Any, List, Mapping, NamedTuple, NewType, Optional, Sequence, Union
from uuid import UUID

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Aggregation, Query
from snuba.query.types import Condition
from snuba.request import Language, Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import (
    build_request,
    parse_legacy_query,
    parse_snql_query_api,
)
from snuba.utils.metrics.timer import Timer

SUBSCRIPTION_REFERRER = "subscription"


class InvalidSubscriptionError(Exception):
    pass


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


class SubscriptionType(Enum):
    LEGACY = "legacy"
    SNQL = "snql"
    DELEGATE = "delegate"


# This is a workaround for a mypy bug, found here: https://github.com/python/mypy/issues/5374
@dataclass(frozen=True)
class _SubscriptionData:
    project_id: int
    resolution: timedelta
    time_window: timedelta


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
        self, dataset: Dataset, timestamp: datetime, offset: Optional[int], timer: Timer
    ) -> Request:
        raise NotImplementedError

    @abstractclassmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SubscriptionData:
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class LegacySubscriptionData(SubscriptionData):
    """
    Represents the state of a legacy subscription (before SnQL).
    """

    conditions: Sequence[Condition]
    aggregations: Sequence[Aggregation]

    def build_request(
        self, dataset: Dataset, timestamp: datetime, offset: Optional[int], timer: Timer
    ) -> Request:
        """
        Returns a Request that can be used to run a query via `parse_and_run_query`.
        :param dataset: The Dataset to build the request for
        :param timestamp: Date that the query should run up until
        :param offset: Maximum offset we should query for
        """
        schema = RequestSchema.build_with_extensions(
            dataset.get_default_entity().get_extensions(),
            SubscriptionRequestSettings,
            Language.LEGACY,
        )
        extra_conditions: Sequence[Condition] = []
        if offset is not None:
            extra_conditions = [[["ifnull", ["offset", 0]], "<=", offset]]
        return build_request(
            {
                "project": self.project_id,
                "conditions": [*self.conditions, *extra_conditions],
                "aggregations": self.aggregations,
                "from_date": (timestamp - self.time_window).isoformat(),
                "to_date": timestamp.isoformat(),
            },
            parse_legacy_query,
            SubscriptionRequestSettings,
            schema,
            dataset,
            timer,
            SUBSCRIPTION_REFERRER,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LegacySubscriptionData:
        if not data.get("aggregations"):
            raise InvalidQueryException("No aggregation provided")

        return LegacySubscriptionData(
            project_id=data["project_id"],
            conditions=data["conditions"],
            aggregations=data["aggregations"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
        )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "project_id": self.project_id,
            "conditions": self.conditions,
            "aggregations": self.aggregations,
            "time_window": int(self.time_window.total_seconds()),
            "resolution": int(self.resolution.total_seconds()),
        }


@dataclass(frozen=True)
class SnQLSubscriptionData(SubscriptionData):
    query: str

    def build_request(
        self, dataset: Dataset, timestamp: datetime, offset: Optional[int], timer: Timer
    ) -> Request:
        schema = RequestSchema.build_with_extensions(
            {}, SubscriptionRequestSettings, Language.SNQL,
        )

        def add_conditions(query: Union[CompositeQuery[Entity], Query]) -> None:
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

            conditions_to_add: List[Expression] = [
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
            ]

            if offset is not None:
                conditions_to_add.append(
                    binary_condition(
                        ConditionFunctions.LTE,
                        FunctionCall(
                            None,
                            "ifnull",
                            (Column(None, None, "offset"), Literal(None, 0)),
                        ),
                        Literal(None, offset),
                    )
                )

            new_condition = combine_and_conditions(conditions_to_add)
            condition = query.get_condition()
            if condition:
                new_condition = binary_condition(
                    BooleanFunctions.AND, condition, new_condition
                )

            query.set_ast_condition(new_condition)

        request = build_request(
            {"query": self.query},
            partial(parse_snql_query_api, [add_conditions]),
            SubscriptionRequestSettings,
            schema,
            dataset,
            timer,
            SUBSCRIPTION_REFERRER,
        )
        return request

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SnQLSubscriptionData:
        if data.get(cls.TYPE_FIELD) != SubscriptionType.SNQL.value:
            raise InvalidQueryException("Invalid SnQL subscription structure")

        return SnQLSubscriptionData(
            project_id=data["project_id"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
            query=data["query"],
        )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            self.TYPE_FIELD: SubscriptionType.SNQL.value,
            "project_id": self.project_id,
            "time_window": int(self.time_window.total_seconds()),
            "resolution": int(self.resolution.total_seconds()),
            "query": self.query,
        }


@dataclass(frozen=True)
class DelegateSubscriptionData(SubscriptionData):
    """
    Embeds two subscription data types for the rollout of SnQL.
    This allows to switch logic back and forth between the
    legacy and the SnQL language.
    """

    # Legacy
    conditions: Sequence[Condition]
    aggregations: Sequence[Aggregation]
    # SnQL
    query: str

    def build_request(
        self, dataset: Dataset, timestamp: datetime, offset: Optional[int], timer: Timer
    ) -> Request:
        # TODO: Switch to SnQL when we do the rollout
        return LegacySubscriptionData(
            project_id=self.project_id,
            resolution=self.resolution,
            time_window=self.time_window,
            conditions=self.conditions,
            aggregations=self.aggregations,
        ).build_request(dataset, timestamp, offset, timer)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DelegateSubscriptionData:
        if data.get(cls.TYPE_FIELD) != SubscriptionType.DELEGATE.value:
            raise InvalidQueryException("Invalid delegate subscription structure")

        return DelegateSubscriptionData(
            project_id=data["project_id"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
            conditions=data["conditions"],
            aggregations=data["aggregations"],
            query=data["query"],
        )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            self.TYPE_FIELD: SubscriptionType.DELEGATE.value,
            "project_id": self.project_id,
            "time_window": int(self.time_window.total_seconds()),
            "resolution": int(self.resolution.total_seconds()),
            "conditions": self.conditions,
            "aggregations": self.aggregations,
            "query": self.query,
        }


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData
