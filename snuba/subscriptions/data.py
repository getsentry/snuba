from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.query.query import Aggregation
from snuba.query.types import Condition
from snuba.request import Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer
from snuba.views import validate_request_content

SUBSCRIPTION_REFERRER = "subscription"


@dataclass(frozen=True)
class Subscription:
    """
    Represents the state of a subscription.
    """

    id: str
    project_id: int
    conditions: Sequence[Condition]
    aggregations: Sequence[Aggregation]
    time_window: timedelta
    resolution: timedelta

    def build_request(
        self, dataset: Dataset, timestamp: datetime, offset: int, timer: Timer
    ) -> Request:
        """
        Returns a Request that can be used to run a query via `parse_and_run_query`.
        :param dataset: The Dataset to build the request for
        :param timestamp: Date that the query should run up until
        :param offset: Maximum offset we should query for
        """
        schema = RequestSchema.build_with_extensions(
            dataset.get_extensions(), SubscriptionRequestSettings,
        )
        return validate_request_content(
            {
                "project": self.project_id,
                "conditions": (
                    self.conditions + [[["ifnull", ["offset", 0]], "<=", offset]]
                ),
                "aggregations": self.aggregations,
                "from_date": (timestamp - self.time_window).isoformat(),
                "to_date": timestamp.isoformat(),
            },
            schema,
            timer,
            dataset,
            SUBSCRIPTION_REFERRER,
        )
