from snuba.clickhouse.processors import QueryProcessor
from snuba.query.conditions import combine_and_conditions
from snuba.query.logical import Query
from snuba.request.request_settings import RequestSettings


class MandatoryConditionApplier(QueryProcessor):

    """
    Obtains mandatory conditions from a Query object’s underlying storage and applies them to both legacy and AST
    query representations, adding on to existing conditions.

    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:

        mandatory_conditions = query.get_data_source().get_mandatory_conditions()
        query.add_conditions([c.legacy for c in mandatory_conditions])

        if len(mandatory_conditions) > 0:
            query.add_condition_to_ast(
                combine_and_conditions([c.ast for c in mandatory_conditions])
            )
