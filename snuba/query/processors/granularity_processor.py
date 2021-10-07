from snuba.datasets.metrics import DEFAULT_GRANULARITY
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class GranularityProcessor(QueryProcessor):
    """ Use the granularity set on the query to filter on the granularity column """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        granularity = query.get_granularity() or DEFAULT_GRANULARITY
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity),
            )
        )
