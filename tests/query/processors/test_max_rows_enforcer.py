import typing
import uuid

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.deletions.max_rows_enforcer import max_rows_enforcer
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.parser import parse_snql_query_initial
from snuba.request import DeleteRequest


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_rows_to_delete_is_less_than_max_rows() -> None:
    occurrence_id = 113059749145936325402354257176981405696  # str(uuid.uuid4())
    delete_request = DeleteRequest(
        id="123",
        query=parse_snql_query_initial(
            f"MATCH STORAGE(search_issues) "
            "SELECT COUNT() "
            "WHERE "
            "occurrence_id = {occurrence_id} AND "
            "project_id = 1 AND "
            "timestamp >= toDateTime('2024-07-17T21:04:34') AND "
            "timestamp < toDateTime('2024-07-17T21:10:34')"
        ),
        tenant_ids={"organization_id": 319976, "referrer": "Group.get_helpful"},
        where_clause=f"WHERE occurrence_id = {occurrence_id} AND project_id = 1 AND timestamp >= toDateTime('2024-07-17T21:04:34') AND timestamp < toDateTime('2024-07-17T21:10:34')",
    )

    max_rows_enforcer(delete_request)
