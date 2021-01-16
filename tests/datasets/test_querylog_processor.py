import uuid
from datetime import datetime

from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.preprocessors import noop_request_processor
from snuba.processor import InsertBatch
from snuba.query.logical import Query
from snuba.querylog.query_metadata import (
    ClickhouseQueryMetadata,
    ClickhouseQueryProfile,
    FilterProfile,
    QueryStatus,
    SnubaQueryMetadata,
)
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.utils.clock import TestingClock
from snuba.utils.metrics.timer import Timer


def test_simple() -> None:
    request_body = {
        "selected_columns": ["event_id"],
        "orderby": "event_id",
        "sample": 0.1,
        "limit": 100,
        "offset": 50,
        "project": 1,
    }

    query = Query(get_storage(StorageKey.EVENTS).get_schema().get_data_source())

    request = Request(
        uuid.UUID("a" * 32).hex,
        request_body,
        query,
        HTTPRequestSettings(),
        "search",
        noop_request_processor,
    )

    time = TestingClock()

    timer = Timer("test", clock=time)
    time.sleep(0.01)

    message = SnubaQueryMetadata(
        request=request,
        dataset="events",
        timer=timer,
        query_list=[
            ClickhouseQueryMetadata(
                sql="select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100",
                stats={"sample": 10},
                status=QueryStatus.SUCCESS,
                profile=ClickhouseQueryProfile(
                    time_range=10,
                    table="events",
                    all_columns={"timestamp", "tags"},
                    multi_level_condition=False,
                    where_profile=FilterProfile(
                        columns={"timestamp"}, mapping_cols={"tags"},
                    ),
                    groupby_cols=set(),
                    array_join_cols=set(),
                ),
                trace_id="b" * 32,
            )
        ],
    ).to_dict()

    processor = (
        get_writable_storage(StorageKey.QUERYLOG)
        .get_table_writer()
        .get_stream_loader()
        .get_processor()
    )

    assert processor.process_message(
        message, KafkaMessageMetadata(0, 0, datetime.now())
    ) == InsertBatch(
        [
            {
                "request_id": str(uuid.UUID("a" * 32)),
                "request_body": '{"limit": 100, "offset": 50, "orderby": "event_id", "project": 1, "sample": 0.1, "selected_columns": ["event_id"]}',
                "referrer": "search",
                "dataset": "events",
                "projects": [1],
                "organization": None,
                "timestamp": timer.for_json()["timestamp"],
                "duration_ms": 10,
                "status": "success",
                "clickhouse_queries.sql": [
                    "select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100"
                ],
                "clickhouse_queries.status": ["success"],
                "clickhouse_queries.trace_id": [str(uuid.UUID("b" * 32))],
                "clickhouse_queries.duration_ms": [0],
                "clickhouse_queries.stats": ['{"sample": 10}'],
                "clickhouse_queries.final": [0],
                "clickhouse_queries.cache_hit": [0],
                "clickhouse_queries.sample": [10.0],
                "clickhouse_queries.max_threads": [0],
                "clickhouse_queries.num_days": [10],
                "clickhouse_queries.clickhouse_table": [""],
                "clickhouse_queries.query_id": [""],
                "clickhouse_queries.is_duplicate": [0],
                "clickhouse_queries.consistent": [0],
                "clickhouse_queries.all_columns": [["tags", "timestamp"]],
                "clickhouse_queries.or_conditions": [False],
                "clickhouse_queries.where_columns": [["timestamp"]],
                "clickhouse_queries.where_mapping_columns": [["tags"]],
                "clickhouse_queries.groupby_columns": [[]],
                "clickhouse_queries.array_join_columns": [[]],
            }
        ],
    )
