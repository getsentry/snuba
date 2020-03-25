from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class TransactionColumnProcessor(QueryProcessor):
    """
    Mimics the old column_expr functionality
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                # TODO remove these casts when clickhouse-driver is >= 0.0.19
                if exp.column_name == "ip_address_v4":
                    return FunctionCall(
                        exp.alias,
                        "IPv4NumToString",
                        (Column(None, "ip_address_v4", None),),
                    )
                if exp.column_name == "ip_address_v6":
                    return FunctionCall(
                        exp.alias,
                        "IPv6NumToString",
                        (Column(None, "ip_address_v6", None),),
                    )
                if exp.column_name == "ip_address":
                    return FunctionCall(
                        exp.alias,
                        "coalesce",
                        (
                            FunctionCall(
                                None,
                                "IPv4NumToString",
                                (Column(None, "ip_address_v4", None),),
                            ),
                            FunctionCall(
                                None,
                                "IPv6NumToString",
                                (Column(None, "ip_address_v6", None),),
                            ),
                        ),
                    )
                if exp.column_name == "event_id":
                    return FunctionCall(
                        exp.alias,
                        "replaceAll",
                        (
                            FunctionCall(
                                None, "toString", (Column(None, "event_id", None),),
                            ),
                            Literal(None, "-"),
                            Literal(None, ""),
                        ),
                    )

            return exp

        query.transform_expressions(process_column)
