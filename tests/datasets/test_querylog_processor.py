import uuid

from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.processor import ProcessedMessage, ProcessorAction
from snuba.query.query import Query
from snuba.request import Request
from snuba.request.schema import HTTPRequestSettings
from snuba.utils.clock import TestingClock
from snuba.utils.metrics.timer import Timer
from snuba.web.query import ClickhouseQueryMetadata, SnubaQueryMetadata


def test_simple():
    request_body = {
        "selected_columns": ["event_id"],
        "orderby": "event_id",
        "sample": 0.1,
        "limit": 100,
        "offset": 50,
        "project": 1,
    }

    query = Query(
        request_body,
        get_dataset("events").get_dataset_schemas().get_read_schema().get_data_source(),
    )

    request = Request(query, HTTPRequestSettings(), {}, "tests")

    time = TestingClock()

    timer = Timer("test", clock=time)
    time.sleep(0.01)

    message = SnubaQueryMetadata(
        query_id=uuid.UUID("a" * 32).hex,
        request=request,
        dataset=get_dataset("events"),
        timer=timer,
        referrer="search",
        query_list=[ClickhouseQueryMetadata(
            sql="select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100",
            stats={"sample": 10},
            status="success",
            trace_id="b" * 32
        )]
    ).to_dict()

    processor = (
        enforce_table_writer(get_dataset("querylog")).get_stream_loader().get_processor()
    )

    assert processor.process_message(message) == ProcessedMessage(
        ProcessorAction.INSERT,
        [{
            "query_id": "a" * 32,
            "request": "{'limit': 100, 'offset': 50, 'orderby': 'event_id', 'project': 1, 'sample': 0.1, 'selected_columns': ['event_id']}",
            "referrer": "search",
            "dataset": get_dataset("events"),
            "projects": [1],
            "organization": None,
            "timestamp": timer.for_json()["timestamp"],
            "duration_ms": 10,
            "status": "success",
            "clickhouse_queries.sql": ["select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100"],
            "clickhouse_queries.status": ["success"],
            "clickhouse_queries.final": [0],
            "clickhouse_queries.cache_hit": [0],
            "clickhouse_queries.sample": [10.],
            "clickhouse_queries.max_threads": [0],
            "clickhouse_queries.trace_id": ["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"],
            "clickhouse_queries.duration_ms": [0],
        }]
    )
