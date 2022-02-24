from __future__ import annotations

from snuba import state
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.request.request_settings import RequestSettings


class UniqKillswitchProcessor(QueryProcessor):
    """
    This processor removes uniq() expressions from the query and replaces them with zero.
    The reason for doing this is because uniq() queries are suspected to be causing ClickHouse to segfault.
    Since it returns incorrect results, it is only to be used in case of emergency.

    Example usage:
    - Enable for all queries to the following tables
        - uniq_killswitch_tables=errors_dist,errors_ro_dist
    - Enable for referrer
        - uniq_killswitch_referrers=referrer1,referrer2,referrer3

    `uniq_killswitch_tables` and `uniq_killswitch_referrers` are independent
    and uniq will be removed from all tables and referrers specified.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        table_config = state.get_config("uniq_killswitch_tables")
        referrer_config = state.get_config("uniq_killswitch_referrers")

        table_names: list[str] = [] if table_config is None else table_config.split(",")
        referrers: list[str] = [] if referrer_config is None else referrer_config.split(
            ","
        )
        table_name = query.get_from_clause().table_name

        should_transform_uniq = (
            request_settings.referrer in referrers or table_name in table_names
        )

        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall):
                if exp.function_name == "uniq":
                    return Literal(exp.alias, 0)

            return exp

        if should_transform_uniq:
            query.transform_expressions(process_functions)
