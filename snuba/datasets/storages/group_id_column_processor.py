from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.query_settings import QuerySettings


class GroupIdColumnProcessor(QueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name == "group_id":
                    return FunctionCall(
                        exp.alias,
                        "nullIf",
                        (
                            Column(None, exp.table_name, exp.column_name),
                            Literal(None, 0),
                        ),
                    )

            return exp

        query.transform_expressions(process_column)
