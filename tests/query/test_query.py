from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.logical import Query


def test_query_parameters():
    query = Query(
        {},
        TableSource("my_table", ColumnSet([])),
        limitby=(100, "environment"),
        sample=10,
        limit=100,
        offset=50,
        totals=True,
        granularity=60,
    )

    assert query.get_limitby() == (100, "environment")
    assert query.get_sample() == 10
    assert query.get_limit() == 100
    assert query.get_offset() == 50
    assert query.has_totals() is True
    assert query.get_granularity() == 60

    assert query.get_from_clause().format_from() == "my_table"
