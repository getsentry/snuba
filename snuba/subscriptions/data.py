from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Sequence

from snuba.datasets.dataset import Dataset
from snuba.query.query import Aggregation
from snuba.query.types import Condition
from snuba.request import Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer

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
        self, dataset: Dataset, timestamp: datetime, offset: Optional[int], timer: Timer
    ) -> Request:
        """
        Returns a Request that can be used to run a query via `parse_and_run_query`.
        :param dataset: The Dataset to build the request for
        :param timestamp: Date that the query should run up until
        :param offset: Maximum offset we should query for
        """
        # XXX: We should move this somewhere better so that we don't need to import from
        # web.views here.
        from snuba.web.views import validate_request_content

        schema = RequestSchema.build_with_extensions(
            dataset.get_extensions(), SubscriptionRequestSettings,
        )
        extra_conditions = []
        if offset is not None:
            extra_conditions = [[["ifnull", ["offset", 0]], "<=", offset]]
        return validate_request_content(
            {
                "project": self.project_id,
                "conditions": self.conditions + extra_conditions,
                "aggregations": self.aggregations,
                "from_date": (timestamp - self.time_window).isoformat(),
                "to_date": timestamp.isoformat(),
            },
            schema,
            timer,
            dataset,
            SUBSCRIPTION_REFERRER,
        )
