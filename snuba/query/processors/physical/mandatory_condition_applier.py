from snuba.clickhouse.query import Query
from snuba.query.conditions import combine_and_conditions
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class MandatoryConditionApplier(ClickhouseQueryProcessor):
    """
    Obtains mandatory conditions from a Query object’s underlying storage
    and applies them to the query.
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        mandatory_conditions = query.get_from_clause().mandatory_conditions

        if len(mandatory_conditions) > 0:
            query.add_condition_to_ast(combine_and_conditions(mandatory_conditions))
