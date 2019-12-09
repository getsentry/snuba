from simplejson import loads

from datetime import datetime

from snuba.datasets.querylog_processor import QueryLogMessageProcessor
from snuba.consumer import KafkaMessageMetadata
from snuba.processor import ProcessorAction


def test_processor() -> None:
    data = (
        '{"request": {"from_date": "2019-12-04T00:20:18", "to_date": "2019-12-09T00:20:18", '
        '"granularity": 3600, "project": [1], "selected_columns": ["event_id"], "aggregations": [], '
        '"conditions": [["timestamp", ">=", "2019-12-04T00:20:18"], '
        '["timestamp", "<", "2019-12-09T00:20:18"], ["deleted", "=", 0]], "groupby": [], '
        '"totals": false, "having": [], "limit": 1000}, '
        '"sql": "SELECT event_id FROM sentry_local PREWHERE project_id IN (1) WHERE timestamp >= '
        "toDateTime('2019-12-04T00:20:18') AND timestamp < toDateTime('2019-12-09T00:20:18') "
        'AND deleted = 0 LIMIT 0, 1000", "timing": {"timestamp": 1575865478, "duration_ms": 81, '
        '"marks_ms": {"cache_get": 0, "dedupe_wait": 0, "execute": 10, "get_configs": 0, '
        '"prepare_query": 18, "rate_limit": 1, "validate_schema": 50}}, "stats": {"from_clause": '
        '"sentry_local", "final": false, "referrer": "http://localhost:1218/events/query", '
        '"num_days": 5, "sample": null, "trace_id": "9d1ffbfa04164d76819edb50acce3a2d", '
        '"dataset": "EventsDataset", "columns": ["deleted", "event_id", "project_id", "timestamp"], '
        '"limit": 1000, "offset": 0, "is_duplicate": false, '
        '"query_id": "1f4a0e38bf753b3bdbdfcac854288a24", "use_cache": false, "cache_hit": false, '
        '"project_rate": 0.016666666666666666, "project_concurrent": 1, "global_rate": 0.0, '
        '"global_concurrent": 1, "consistent": false, "result_rows": 1000, "result_cols": 1}, '
        '"status": 200}'
    )

    expected = {
        "clickhouse_query": (
            "SELECT event_id FROM sentry_local PREWHERE project_id IN (1) WHERE timestamp >= "
            "toDateTime('2019-12-04T00:20:18') AND timestamp < toDateTime('2019-12-09T00:20:18') "
            "AND deleted = 0 LIMIT 0, 1000"
        ),
        "snuba_query": (
            '{"from_date": "2019-12-04T00:20:18", "to_date": "2019-12-09T00:20:18", '
            '"granularity": 3600, "project": [1], "selected_columns": ["event_id"], "aggregations": [], '
            '"conditions": [["timestamp", ">=", "2019-12-04T00:20:18"], '
            '["timestamp", "<", "2019-12-09T00:20:18"], ["deleted", "=", 0]], "groupby": [], '
            '"totals": false, "having": [], "limit": 1000}'
        ),
        "timestamp": datetime.fromtimestamp(1575865478),
        "duration_ms": 81,
        "referrer": "http://localhost:1218/events/query",
        "hits": 1,
        "trace_id": "9d1ffbfa-0416-4d76-819e-db50acce3a2d",
        "dataset": "EventsDataset",
        "from_clause": "sentry_local",
        "columns": ["deleted", "event_id", "project_id", "timestamp"],
        "limit": 1000,
        "offset": None,
        "tags.key": [
            "cache_hit",
            "consistent",
            "final",
            "global_concurrent",
            "global_rate",
            "is_duplicate",
            "num_days",
            "project_concurrent",
            "project_rate",
            "query_id",
            "result_cols",
            "result_rows",
            "use_cache",
        ],
        "tags.value": [
            "False",
            "False",
            "False",
            "1",
            "0.0",
            "False",
            "5",
            "1",
            "0.016666666666666666",
            "1f4a0e38bf753b3bdbdfcac854288a24",
            "1",
            "1000",
            "False",
        ],
    }

    meta = KafkaMessageMetadata(offset=1, partition=0)
    processor = QueryLogMessageProcessor()
    ret = processor.process_message(loads(data), meta)

    assert ret.action == ProcessorAction.INSERT
    assert ret.data == [expected]
