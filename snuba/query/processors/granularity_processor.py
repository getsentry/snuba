from typing import Sequence, Tuple

from snuba.datasets.metrics import DEFAULT_GRANULARITY
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.query_settings import QuerySettings

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

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        granularity = self.__get_granularity(query)
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity),
            )
        )


PERFORMANCE_GRANULARITIES = [(60, 1), (3600, 2), (86400, 3)]
DEFAULT_MAPPED_GRANULARITY_ENUM = 1


class MappedGranularityProcessor(QueryProcessor):
    """
    Use the granularity set on the query to filter on the granularity column,
    supporting generic-metrics style enum mapping
    """

    def __init__(
        self,
        accepted_granularities: Sequence[Tuple[int, int]],
        default_granularity_enum: int,
    ):
        self._accepted_granularities = accepted_granularities
        self._available_granularities_values = [
            x[0] for x in self._accepted_granularities
        ]
        self._default_granularity_enum = default_granularity_enum

    def __get_granularity(self, query: Query) -> int:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()

        if requested_granularity is None:
            return self._default_granularity_enum
        elif requested_granularity > 0:
            for (granularity, mapped_value) in reversed(self._accepted_granularities):
                if (
                    requested_granularity % granularity == 0
                    or requested_granularity == mapped_value
                ):
                    return mapped_value

        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {self._available_granularities_values}"
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        granularity = self.__get_granularity(query)
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity),
            )
        )
