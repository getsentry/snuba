from typing import Optional

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.query_settings import QuerySettings
from snuba.state import get_configs
from snuba.state.rate_limit import TABLE_RATE_LIMIT_NAME, RateLimitParameters


class TableRateLimit(QueryProcessor):
    """
    Set a rate limiter for individual tables.
    TODO: Do this at Cluster level instead.
    """

    def __init__(self, suffix: Optional[str] = None) -> None:
        self.__suffix = "_".join(["", suffix]) if suffix else ""

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        table_name = query.get_from_clause().table_name
        (per_second, concurr) = get_configs(
            [
                (f"table_per_second_limit_{table_name}{self.__suffix}", 5000),
                (f"table_concurrent_limit_{table_name}{self.__suffix}", 1000),
            ]
        )

        rate_limit = RateLimitParameters(
            rate_limit_name=TABLE_RATE_LIMIT_NAME,
            bucket=table_name,
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )

        query_settings.add_rate_limit(rate_limit)
