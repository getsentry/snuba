import pytest

from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_basic():
    request, result = parse_and_run_query(
        body={
            "query": "MATCH (events) SELECT event_id, group_id, project_id, timestamp WHERE timestamp >= toDateTime('2024-07-17T21:04:34') AND timestamp < toDateTime('2024-07-17T21:10:34') AND project_id = 1",
            "tenant_ids": {"organization_id": 319976, "referrer": "Group.get_helpful"},
        },
        timer=Timer("parse_and_run_query"),
        is_mql=False,
    )
    assert result.result["meta"] == [
        {"name": "event_id", "type": "String"},
        {"name": "group_id", "type": "UInt64"},
        {"name": "project_id", "type": "UInt64"},
        {"name": "timestamp", "type": "DateTime"},
    ]
