from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import binary_condition, ConditionFunctions
from snuba.query.expressions import Column, Literal
from snuba.request.request_settings import RequestSettings


class TypeConditionEnforcer(QueryProcessor):
    """
    Enforces that transactions are never returned from the events entity.
    This condition is required for the events storage only, transactions are never
    present in errors storage. If the query contains a (likely more restrictive)
    condition on group ID, do not apply the additional condition.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        cols = {
            col.column_name for col in query.get_columns_referenced_in_conditions_ast()
        }

        if "group_id" not in cols:
            query.add_condition_to_ast(
                binary_condition(
                    ConditionFunctions.NEQ,
                    Column("_snuba_type", None, "type"),
                    Literal(None, "transaction"),
                )
            )
