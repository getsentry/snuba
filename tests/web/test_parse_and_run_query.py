import pytest

from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_basic_snql() -> None:
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


@pytest.mark.genmetrics_db
@pytest.mark.redis_db
def test_basic_mql() -> None:
    # body = {'debug': True, 'query': 'sum(c:transactions/count_per_root_project@none){transaction:"t1"} by (status_code)', 'dataset': 'generic_metrics', 'app_id': 'test', 'tenant_ids': {'referrer': 'tests', 'organization_id': 101}, 'parent_api': '<unknown>', 'mql_context': {'start': '2024-07-16T09:15:00+00:00', 'end': '2024-07-16T15:15:00+00:00', 'rollup': {'orderby': None, 'granularity': 60, 'interval': 60, 'with_totals': None}, 'scope': {'org_ids': [101], 'project_ids': [1, 2], 'use_case_id': 'performance'}, 'indexer_mappings': {'transaction.duration': 'c:transactions/count_per_root_project@none', 'c:transactions/count_per_root_project@none': 1067, 'transaction': 65546, 'status_code': 9223372036854776010}, 'limit': None, 'offset': None}}
    body = {
        "query": 'sum(c:transactions/count_per_root_project@none){transaction:"t1"} by (status_code)',
        "dataset": "generic_metrics",
        "app_id": "test",
        "tenant_ids": {"referrer": "tests", "organization_id": 101},
        "parent_api": "<unknown>",
        "mql_context": {
            "start": "2024-07-16T09:15:00+00:00",
            "end": "2024-07-16T15:15:00+00:00",
            "rollup": {
                "orderby": None,
                "granularity": 60,
                "interval": 60,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [101],
                "project_ids": [1, 2],
                "use_case_id": "performance",
            },
            "indexer_mappings": {
                "transaction.duration": "c:transactions/count_per_root_project@none",
                "c:transactions/count_per_root_project@none": 1067,
                "transaction": 65546,
                "status_code": 9223372036854776010,
            },
            "limit": None,
            "offset": None,
        },
    }
    request, result = parse_and_run_query(
        body=body,
        timer=Timer("parse_and_run_query"),
        is_mql=True,
        dataset_name="generic_metrics",
    )
