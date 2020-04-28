from snuba.clickhouse.query import Query
from snuba.datasets.storages.processors import QueryProcessor
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.request.request_settings import RequestSettings


class EventsColumnProcessor(QueryProcessor[Query]):
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
                            Column(None, exp.column_name, exp.table_name),
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
                            Column(None, exp.column_name, exp.table_name),
                            Column(None, "search_message", exp.table_name),
                        ),
                    )

            return exp

        query.transform_expressions(process_column)
