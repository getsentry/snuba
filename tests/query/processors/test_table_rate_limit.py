import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query.data_source.simple import Table
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import set_config
from snuba.state.rate_limit import TABLE_RATE_LIMIT_NAME, RateLimitParameters

test_data = [
    pytest.param(
        Query(
            Table("errors_local", ColumnSet([])), selected_columns=[], condition=None
        ),
        "table_concurrent_limit_transactions_local",
        RateLimitParameters(
            rate_limit_name=TABLE_RATE_LIMIT_NAME,
            bucket="errors_local",
            per_second_limit=1000,
            concurrent_limit=1000,
        ),
        id="Set rate limiter on another table",
    ),
    pytest.param(
        Query(
            Table("errors_local", ColumnSet([])), selected_columns=[], condition=None
        ),
        "table_concurrent_limit_errors_local",
        RateLimitParameters(
            rate_limit_name=TABLE_RATE_LIMIT_NAME,
            bucket="errors_local",
            per_second_limit=1000,
            concurrent_limit=50,
        ),
        id="Set rate limiter on another table",
    ),
]


@pytest.mark.parametrize("query, limit_to_set, params", test_data)
def test_table_rate_limit(
    query: Query, limit_to_set: str, params: RateLimitParameters
) -> None:
    set_config(limit_to_set, 50)
    query_settings = HTTPQuerySettings(consistent=True)
    TableRateLimit().process_query(query, query_settings)
    rate_limiters = query_settings.get_rate_limit_params()
    assert params in rate_limiters
