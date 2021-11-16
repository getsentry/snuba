from datetime import timedelta
from typing import Optional

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.metrics import DEFAULT_GRANULARITY
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings, SubscriptionRequestSettings

#: Granularities for which a materialized view exist, in ascending order
GRANULARITIES_AVAILABLE = (10, 60, 60 * 60, 24 * 60 * 60)


class GranularityProcessor(QueryProcessor):
    """Use the granularity set on the query to filter on the granularity column"""

    @staticmethod
    def __get_granularity(query: Query) -> int:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()

        if requested_granularity is None:
            return DEFAULT_GRANULARITY
        elif requested_granularity > 0:
            for granularity in reversed(GRANULARITIES_AVAILABLE):
                if (requested_granularity % granularity) == 0:

                    return granularity

        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {GRANULARITIES_AVAILABLE}"
        )

    @staticmethod
    def __get_granularity_for_subscription(query: Query) -> Optional[int]:
        granularity = None
        entity = get_entity(query.get_from_clause().key)
        if not entity.required_time_column:
            raise InvalidGranularityException(
                "Entity mush have a time column defined to be able to calculate granularity"
            )
        from_date, to_date = get_time_range(query, entity.required_time_column)
        if from_date and to_date:
            time_interval = to_date - from_date
            if time_interval <= timedelta(minutes=60):
                granularity = 10
            elif time_interval <= timedelta(hours=4):
                granularity = 60
            else:
                granularity = 60 * 60
        return granularity

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        granularity = None
        if isinstance(request_settings, SubscriptionRequestSettings):
            granularity = self.__get_granularity_for_subscription(query)
        if granularity is None:
            granularity = self.__get_granularity(query)

        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity),
            )
        )
