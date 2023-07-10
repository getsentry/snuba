import uuid
from copy import deepcopy
from datetime import datetime, timedelta

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.querylog.query_metadata import (
    ClickhouseQueryMetadata,
    ClickhouseQueryProfile,
    FilterProfile,
    RequestStatus,
    SnubaQueryMetadata,
    Status,
)
from snuba.request import Request
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

    query = Query(
        Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model())
    )

    request = Request(
        id=uuid.UUID("a" * 32).hex,
        original_body=request_body,
        query=query,
        snql_anonymized="",
        query_settings=HTTPQuerySettings(referrer="search"),
        attribution_info=AttributionInfo(
            get_app_id("default"),
            {"tenant_type": "tenant_id"},
            "search",
            None,
            None,
            None,
        ),
    )

    time = TestingClock()

    timer = Timer("test", clock=time)
    time.sleep(0.01)

    message = SnubaQueryMetadata(
        request=request,
        start_timestamp=datetime.utcnow() - timedelta(days=3),
        end_timestamp=datetime.utcnow(),
        dataset="events",
        timer=timer,
        query_list=[
            ClickhouseQueryMetadata(
                sql="select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100",
                sql_anonymized="select event_id from sentry_dist sample 0.1 prewhere project_id in ($I) limit 50, 100",
                start_timestamp=datetime.utcnow() - timedelta(days=3),
                end_timestamp=datetime.utcnow(),
                stats={
                    "sample": 10,
                    "error_code": 386,
                    "triggered_rate_limiter": "test_rate_limiter",
                },
                request_status=Status(RequestStatus.SUCCESS),
                profile=ClickhouseQueryProfile(
                    time_range=10,
                    table="events",
                    all_columns={"timestamp", "tags"},
                    multi_level_condition=False,
                    where_profile=FilterProfile(
                        columns={"timestamp"},
                        mapping_cols={"tags"},
                    ),
                    groupby_cols=set(),
                    array_join_cols=set(),
                ),
                result_profile={"bytes": 1337, "elapsed": 0.042},
                trace_id="b" * 32,
            )
        ],
        projects={2},
        snql_anonymized=request.snql_anonymized,
        entity=EntityKey.EVENTS.value,
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
                "projects": [2],
                "organization": None,
                "timestamp": timer.for_json()["timestamp"],
                "duration_ms": 10,
                "status": "success",
                "clickhouse_queries.sql": [
                    "select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100"
                ],
                "clickhouse_queries.status": ["success"],
                "clickhouse_queries.trace_id": [str(uuid.UUID("b" * 32))],
                "clickhouse_queries.duration_ms": [42],
                "clickhouse_queries.stats": [
                    '{"error_code": 386, "sample": 10, "triggered_rate_limiter": "test_rate_limiter"}'
                ],
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
                "clickhouse_queries.bytes_scanned": [1337],
            }
        ],
        None,
    )


def test_missing_fields() -> None:
    request_body = {
        "selected_columns": ["event_id"],
        "orderby": "event_id",
        "sample": 0.1,
        "limit": 100,
        "offset": 50,
        "project": 1,
    }

    query = Query(
        Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model())
    )

    request = Request(
        id=uuid.UUID("a" * 32).hex,
        original_body=request_body,
        query=query,
        snql_anonymized="",
        query_settings=HTTPQuerySettings(referrer="search"),
        attribution_info=AttributionInfo(
            get_app_id("default"),
            {"tenant_type": "tenant_id"},
            "search",
            None,
            None,
            None,
        ),
    )

    time = TestingClock()

    timer = Timer("test", clock=time)
    time.sleep(0.01)

    orig_message = SnubaQueryMetadata(
        request=request,
        start_timestamp=None,
        end_timestamp=None,
        dataset="events",
        timer=timer,
        query_list=[
            ClickhouseQueryMetadata(
                sql="select event_id from sentry_dist sample 0.1 prewhere project_id in (1) limit 50, 100",
                sql_anonymized="select event_id from sentry_dist sample 0.1 prewhere project_id in ($I) limit 50, 100",
                start_timestamp=None,
                end_timestamp=None,
                stats={"sample": 10},
                request_status=Status(RequestStatus.SUCCESS),
                profile=ClickhouseQueryProfile(
                    time_range=10,
                    table="events",
                    all_columns={"timestamp", "tags"},
                    multi_level_condition=False,
                    where_profile=FilterProfile(
                        columns={"timestamp"},
                        mapping_cols={"tags"},
                    ),
                    groupby_cols=set(),
                    array_join_cols=set(),
                ),
                trace_id="b" * 32,
            )
        ],
        projects={2},
        snql_anonymized=request.snql_anonymized,
        entity=EntityKey.EVENTS.value,
    ).to_dict()

    messages = []
    first = dict(deepcopy(orig_message))
    del first["timing"]
    del first["status"]
    messages.append(first)

    second = dict(deepcopy(orig_message))
    second["timing"] = None
    second["status"] = None
    messages.append(second)

    for message in messages:
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
                    "projects": [2],
                    "organization": None,
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
                    "clickhouse_queries.bytes_scanned": [0],
                }
            ],
            None,
        )


def test_negative_project_id_fields() -> None:
    request_body = {
        "selected_columns": ["event_id"],
        "orderby": "event_id",
        "sample": 0.1,
        "limit": 100,
        "offset": 50,
        "project": -11,
    }

    query = Query(
        Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model())
    )

    request = Request(
        id=uuid.UUID("a" * 32).hex,
        original_body=request_body,
        query=query,
        snql_anonymized="",
        query_settings=HTTPQuerySettings(referrer="search"),
        attribution_info=AttributionInfo(
            get_app_id("default"),
            {"tenant_type": "tenant_id"},
            "search",
            None,
            None,
            None,
        ),
    )

    time = TestingClock()

    timer = Timer("test", clock=time)
    time.sleep(0.01)

    message = SnubaQueryMetadata(
        request=request,
        start_timestamp=None,
        end_timestamp=None,
        dataset="events",
        timer=timer,
        query_list=[
            ClickhouseQueryMetadata(
                sql="select event_id from sentry_dist sample 0.1 prewhere project_id in (-11) limit 50, 100",
                sql_anonymized="select event_id from sentry_dist sample 0.1 prewhere project_id in ($I) limit 50, 100",
                start_timestamp=None,
                end_timestamp=None,
                stats={"sample": 10},
                request_status=Status(RequestStatus.SUCCESS),
                profile=ClickhouseQueryProfile(
                    time_range=10,
                    table="events",
                    all_columns={"timestamp", "tags"},
                    multi_level_condition=False,
                    where_profile=FilterProfile(
                        columns={"timestamp"},
                        mapping_cols={"tags"},
                    ),
                    groupby_cols=set(),
                    array_join_cols=set(),
                ),
                trace_id="b" * 32,
            )
        ],
        projects={-2, 0, 420},
        snql_anonymized=request.snql_anonymized,
        entity=EntityKey.EVENTS.value,
    ).to_dict()

    processor = (
        get_writable_storage(StorageKey.QUERYLOG)
        .get_table_writer()
        .get_stream_loader()
        .get_processor()
    )
    res_batch = processor.process_message(
        message, KafkaMessageMetadata(0, 0, datetime.now())
    )
    # We keep only valid project ids (>= 0)
    assert isinstance(res_batch, InsertBatch)
    assert res_batch.rows[0]["projects"] == [420]
