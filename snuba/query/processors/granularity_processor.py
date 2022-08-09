from typing import NamedTuple, Sequence, Union

from snuba.datasets.metrics import DEFAULT_GRANULARITY
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor, query_processor
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


class GranularityMapping(NamedTuple):
    raw: int
    enum_value: int


PERFORMANCE_GRANULARITIES: Sequence[GranularityMapping] = [
    GranularityMapping(60, 1),
    GranularityMapping(3600, 2),
    GranularityMapping(86400, 3),
]
DEFAULT_MAPPED_GRANULARITY_ENUM = 1


@query_processor("handle_mapped_granularities")
class MappedGranularityProcessor(QueryProcessor):
    """
    Use the granularity set on the query to filter on the granularity column,
    supporting generic-metrics style enum mapping
    """

    def __init__(
        self,
        accepted_granularities: Union[
            Sequence[GranularityMapping], Sequence[Sequence[int]]
        ],
        default_granularity_enum: int,
    ):
        # handle YAML not knowing about the GranularityMapping NamedTuple
        accepted_granularities_processed: Sequence[GranularityMapping] = []
        if isinstance(accepted_granularities[0], Sequence):
            accepted_granularities_processed = [
                GranularityMapping(raw=seq[0], enum_value=seq[1])
                for seq in accepted_granularities
            ]
        else:
            # this is dumb but I wanted to shut mypy up
            accepted_granularities_processed = [
                g for g in accepted_granularities if isinstance(g, GranularityMapping)
            ]

        self._accepted_granularities = sorted(
            accepted_granularities_processed,
            key=lambda mapping: mapping.raw,
            reverse=True,
        )
        self._available_granularities_values = [
            mapping.raw for mapping in self._accepted_granularities
        ]
        self._default_granularity_enum = default_granularity_enum

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
