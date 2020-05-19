from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.request.request_settings import RequestSettings


class EventsColumnProcessor(QueryProcessor):
    """
    Strip any dashes out of the event ID to match what is stored internally.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
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
                elif exp.column_name == "message":
                    # Because of the rename from message->search_message without backfill,
                    # records will have one or the other of these fields.
                    # TODO this can be removed once all data has search_message filled in.
                    return FunctionCall(
                        exp.alias,
                        "coalesce",
                        (
                            Column(None, exp.table_name, exp.column_name),
                            Column(None, exp.table_name, "search_message"),
                        ),
                    )

            return exp

        query.transform_expressions(process_column)
