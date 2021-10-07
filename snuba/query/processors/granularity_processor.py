from snuba.datasets.metrics import DEFAULT_GRANULARITY
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings

#: Granularities for which a materialized view exist, in ascending order
GRANULARITIES_AVAILABLE = (10, 60)


class GranularityProcessor(QueryProcessor):
    """ Use the granularity set on the query to filter on the granularity column """

    @staticmethod
    def __get_granularity(query: Query) -> int:
        """ Find the best fitting granularity for this query """
        requested_granularity = query.get_granularity()
        if requested_granularity:
            for granularity in reversed(GRANULARITIES_AVAILABLE):
                if (requested_granularity % granularity) == 0:

                    return granularity

        return DEFAULT_GRANULARITY

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        granularity = self.__get_granularity(query)
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity),
            )
        )
