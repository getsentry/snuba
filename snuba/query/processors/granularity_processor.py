from typing import Mapping, NamedTuple

from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.query_settings import QuerySettings

#: Granularities for which a materialized view exist, in ascending order
GRANULARITIES_AVAILABLE = (10, 60, 60 * 60, 24 * 60 * 60)
DEFAULT_GRANULARITY_RAW = 60


class GranularityProcessor(QueryProcessor):
    """Use the granularity set on the query to filter on the granularity column"""

    @staticmethod
    def __get_granularity(query: Query) -> int:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()

        if requested_granularity is None:
            return DEFAULT_GRANULARITY_RAW
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


class GranularityMapping(NamedTuple):
    raw: int
    enum_value: int


PERFORMANCE_GRANULARITIES: Mapping[int, int] = {
    60: 1,
    3600: 2,
    86400: 3,
}
DEFAULT_MAPPED_GRANULARITY_ENUM = 1


class MappedGranularityProcessor(QueryProcessor):
    """
    Use the granularity set on the query to filter on the granularity column,
    supporting generic-metrics style enum mapping (e.g. input granularity of 60s
    is mapped to the enum granularity of 1)
    """

    def __init__(
        self,
        accepted_granularities: Mapping[int, int],
        default_granularity: int,
    ):
        """
        Constructs a new MappedGranularityProcessor

        :param accepted_granularities: a Mapping of raw to enumerated-value granularities
            where the key is the granularity we expect from user input and the value is
            the granularity we expect to see in the table/query
        :param default_granularity: the default granularity value (as seen by the table)
            to use in the query if the user does not supply one
        """
        accepted_granularities_processed = [
            GranularityMapping(k, v) for (k, v) in accepted_granularities.items()
        ]
        self._accepted_granularities = sorted(
            accepted_granularities_processed,
            key=lambda mapping: mapping.raw,
            reverse=True,
        )
        self._available_granularities_values = [
            mapping.raw for mapping in self._accepted_granularities
        ]
        self._default_granularity_enum = default_granularity

    def __get_granularity(self, query: Query) -> int:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()

        if requested_granularity is None:
            return self._default_granularity_enum
        elif requested_granularity > 0:
            for mapping in self._accepted_granularities:
                if requested_granularity % mapping.raw == 0:
                    return mapping.enum_value

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
