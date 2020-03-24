from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.expressions import (
    Expression,
    Column,
)
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.processors.performance_expressions import apdex
from snuba.request.request_settings import RequestSettings


class TransactionColumnProcessor(QueryProcessor):
    """
    Mimics the old column_expr functionality
    """

    def __init__(self, tags_processor: TagColumnProcessor) -> None:
        self.tags_processor = tags_processor

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                # TODO remove these casts when clickhouse-driver is >= 0.0.19
                if exp.column_name == "ip_address_v4":
                    return Column(
                        exp.alias, "IPv4NumToString(ip_address_v4)", exp.table_name
                    )
                if exp.column_name == "ip_address_v6":
                    return Column(
                        exp.alias, "IPv6NumToString(ip_address_v6)", exp.table_name
                    )
                if exp.column_name == "ip_address":
                    return Column(
                        exp.alias,
                        f"coalesce(IPv4NumToString(ip_address_v4), IPv6NumToString(ip_address_v6))",
                        exp.table_name,
                    )
                if exp.column_name == "event_id":
                    return Column(
                        exp.alias,
                        "replaceAll(toString(event_id), '-', '')",
                        exp.table_name,
                    )

                processed_column_name = self.tags_processor.process_column_expression(
                    exp.column_name, query, ParsingContext(),
                )
                if processed_column_name:
                    return Column(exp.alias, processed_column_name, exp.table_name)
                    
            return exp

        query.transform_expressions(process_column)
