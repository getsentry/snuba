from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import NamedTuple, NewType, Optional, Sequence
from uuid import UUID

from snuba.datasets.dataset import Dataset
from snuba.query.logical import Aggregation
from snuba.query.types import Condition
from snuba.request import Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request
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


@dataclass(frozen=True)
class SubscriptionData:
    """
    Represents the state of a subscription.
    """

    project_id: int
    conditions: Sequence[Condition]
    aggregations: Sequence[Aggregation]
    time_window: timedelta
    resolution: timedelta

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
            dataset.get_default_entity().get_extensions(), SubscriptionRequestSettings
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
            schema,
            timer,
            dataset,
            SUBSCRIPTION_REFERRER,
        )


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData
