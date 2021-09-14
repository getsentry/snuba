from __future__ import annotations

import logging
import random
import re
from abc import ABC, abstractclassmethod, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property, partial
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    Union,
)
from uuid import UUID

from snuba import state
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
from snuba.query.validation.validators import (
    NoTimeBasedConditionValidator,
    SubscriptionAllowedClausesValidator,
)
from snuba.request import Language, Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_legacy_query, parse_snql_query
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer

SUBSCRIPTION_REFERRER = "subscription"
CRASH_RATE_ALERT_PATTERN = re.compile(r"(.*)crash_free_percentage")

logger = logging.getLogger("snuba.subscriptions")


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
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
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
    organization: Optional[int] = None
    limit: int = 1
    offset: int = 0
    granularity: int = 3600

    @staticmethod
    def has_crash_rate_alert_aggregation(aggregations: Sequence[Aggregation]) -> bool:
        for aggregation in aggregations:
            try:
                if aggregation[2] and bool(
                    CRASH_RATE_ALERT_PATTERN.match(aggregation[2])
                ):
                    return True
            except IndexError:
                continue
        return False

    @classmethod
    def update_sessions_subscriptions_with_session_params(
        cls, data_dict: Dict[str, Any], data: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Function that checks if this instance of LegacySubscriptionData is for a Crash Rate Alert
        by checking if there is a `crash_free_percentage` aggregation, and if so updates the data
        dictionary used to create an instance from this class.
        """
        if cls.has_crash_rate_alert_aggregation(data.get("aggregations", [])):
            if not data.get("organization"):
                raise InvalidQueryException(
                    "No organization provided for Sessions subscription"
                )

            # Querying over the sessions dataset requires adding the following required fields:
            # organization, offset and limit.
            # In the case of subscriptions, we want to be able to send the granularity to snuba from
            # sentry as it changes from one time window to the other. Specifically, we expect to use
            # one minute granularity for time windows less than or equal to 1h and the one hour
            # granularity for any time window above that
            data_dict.update(
                {
                    "organization": data["organization"],
                    "limit": data.get("limit", 1),
                    "offset": data.get("offset", 0),
                    "granularity": data.get("granularity", 3600),
                }
            )
        return data_dict

    @cached_property
    def is_crash_rate_alert_subscription(self) -> bool:
        return self.has_crash_rate_alert_aggregation(self.aggregations)

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
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

        build_request_dict: Dict[str, Any] = {}
        build_request_dict = self.update_sessions_subscriptions_with_session_params(
            data_dict=build_request_dict,
            data={
                "aggregations": self.aggregations,
                "granularity": self.granularity,
                "organization": self.organization,
                "offset": self.offset,
                "limit": self.limit,
            },
        )

        # This check is necessary because the `sessions` dataset does not contain a column `offset`
        if not self.is_crash_rate_alert_subscription and offset is not None:
            extra_conditions = [[["ifnull", ["offset", 0]], "<=", offset]]

        build_request_dict.update(
            {
                "project": self.project_id,
                "conditions": [*self.conditions, *extra_conditions],
                "aggregations": self.aggregations,
                "from_date": (timestamp - self.time_window).isoformat(),
                "to_date": timestamp.isoformat(),
            }
        )

        return build_request(
            build_request_dict,
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

        legacy_subs_dict = {
            "project_id": data["project_id"],
            "conditions": data["conditions"],
            "aggregations": data["aggregations"],
            "time_window": timedelta(seconds=data["time_window"]),
            "resolution": timedelta(seconds=data["resolution"]),
        }
        legacy_subs_dict = cls.update_sessions_subscriptions_with_session_params(
            data_dict=legacy_subs_dict, data=data
        )
        return LegacySubscriptionData(**legacy_subs_dict)

    def to_dict(self) -> Mapping[str, Any]:
        legacy_subs_dict = {
            "project_id": self.project_id,
            "conditions": self.conditions,
            "aggregations": self.aggregations,
            "time_window": int(self.time_window.total_seconds()),
            "resolution": int(self.resolution.total_seconds()),
        }
        if self.is_crash_rate_alert_subscription:
            legacy_subs_dict.update(
                {
                    "organization": self.organization,
                    "granularity": self.granularity,
                    "offset": self.offset,
                    "limit": self.limit,
                }
            )

        return legacy_subs_dict


@dataclass(frozen=True)
class SnQLSubscriptionData(SubscriptionData):
    query: str

    @cached_property
    def is_crash_rate_alert_subscription(self) -> bool:
        return bool(CRASH_RATE_ALERT_PATTERN.match(self.query))

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

        # This check is necessary because the `sessions` dataset does not have an `offset` column
        if not self.is_crash_rate_alert_subscription and offset is not None:
            conditions_to_add.append(
                binary_condition(
                    ConditionFunctions.LTE,
                    FunctionCall(
                        None,
                        "ifNull",
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

    def validate_subscription(
        self, query: Union[CompositeQuery[Entity], Query]
    ) -> None:
        # TODO: Support composite queries with multiple entities.
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, Entity):
            raise InvalidSubscriptionError("Only simple queries are supported")
        entity = get_entity(from_clause.key)

        SubscriptionAllowedClausesValidator().validate(query)
        if entity.required_time_column:
            NoTimeBasedConditionValidator(entity.required_time_column).validate(query)

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
    ) -> Request:
        schema = RequestSchema.build_with_extensions(
            {}, SubscriptionRequestSettings, Language.SNQL,
        )

        request = build_request(
            {"query": self.query},
            partial(
                parse_snql_query,
                [
                    self.validate_subscription,
                    partial(self.add_conditions, timestamp, offset),
                ],
            ),
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

    # SnQL
    query: str

    # Legacy
    conditions: Sequence[Condition]
    aggregations: Sequence[Aggregation]
    organization: Optional[int] = None
    limit: int = 1
    offset: int = 0
    granularity: int = 3600

    @cached_property
    def is_crash_rate_alert_subscription(self) -> bool:
        return LegacySubscriptionData.has_crash_rate_alert_aggregation(
            self.aggregations
        )

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
    ) -> Request:
        try:
            if metrics is not None:
                metrics.increment("snql.subscription.delegate.incoming")
            snql_rollout_pct = state.get_config("snql_subscription_rollout_pct", 1.0)
            assert isinstance(snql_rollout_pct, float)

            use_snql = snql_rollout_pct > 0.0 and random.random() <= snql_rollout_pct
            if use_snql:
                if metrics is not None:
                    metrics.increment("snql.subscription.delegate.use_snql")
                return self.to_snql().build_request(dataset, timestamp, offset, timer)
        except Exception as e:
            if metrics is not None:
                metrics.increment("snql.subscription.delegate.error")
            logger.warning(
                f"failed snql subscription: {e}",
                exc_info=e,
                extra={
                    "error": str(e),
                    "project": self.project_id,
                    "query": self.query,
                },
            )

        if metrics is not None:
            metrics.increment("snql.subscription.delegate.use_legacy")

        return self.to_legacy().build_request(dataset, timestamp, offset, timer)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DelegateSubscriptionData:
        if data.get(cls.TYPE_FIELD) != SubscriptionType.DELEGATE.value:
            raise InvalidQueryException("Invalid delegate subscription structure")

        delegate_subs_dict = {
            "project_id": data["project_id"],
            "time_window": timedelta(seconds=data["time_window"]),
            "resolution": timedelta(seconds=data["resolution"]),
            "conditions": data["conditions"],
            "aggregations": data["aggregations"],
            "query": data["query"],
        }

        delegate_subs_dict = LegacySubscriptionData.update_sessions_subscriptions_with_session_params(
            data_dict=delegate_subs_dict, data=data
        )

        return DelegateSubscriptionData(**delegate_subs_dict)

    def to_dict(self) -> Mapping[str, Any]:
        delegate_subs_dict = {
            self.TYPE_FIELD: SubscriptionType.DELEGATE.value,
            "project_id": self.project_id,
            "time_window": int(self.time_window.total_seconds()),
            "resolution": int(self.resolution.total_seconds()),
            "conditions": self.conditions,
            "aggregations": self.aggregations,
            "query": self.query,
        }

        if self.is_crash_rate_alert_subscription:
            delegate_subs_dict.update(
                {
                    "organization": self.organization,
                    "limit": self.limit,
                    "offset": self.offset,
                    "granularity": self.granularity,
                }
            )
        return delegate_subs_dict

    def to_snql(self) -> SnQLSubscriptionData:
        return SnQLSubscriptionData.from_dict(
            {**self.to_dict(), self.TYPE_FIELD: SubscriptionType.SNQL.value}
        )

    def to_legacy(self) -> LegacySubscriptionData:
        return LegacySubscriptionData.from_dict(
            {**self.to_dict(), self.TYPE_FIELD: SubscriptionType.LEGACY.value}
        )


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData
