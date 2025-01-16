import uuid
from datetime import UTC, datetime, timedelta, timezone
from time import sleep
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
    TraceItemAttributeValuesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job
from snuba.manual_jobs.scrub_users_from_eap_spans_str_attrs import (
    ScrubUserFromEAPSpansStrAttrs,
)
from snuba.web.rpc.v1.trace_item_attribute_values import AttributeValuesRequest
from tests.helpers import write_raw_unprocessed_events


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_basic() -> None:
    job_id = "abc"
    run_job(
        JobSpec(
            job_id,
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, 3, 5, 6],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        )
    )

    assert get_job_status(job_id) == JobStatus.FINISHED


@pytest.mark.parametrize(
    ("jobspec"),
    [
        JobSpec(
            "abc",
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, "b"],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, 2],
                "start_datetime": "2024-12-01 00:00:0",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, 2],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:0",
            },
        ),
    ],
)
@pytest.mark.redis_db
def test_fail_validation(jobspec: JobSpec) -> None:
    with pytest.raises(Exception):
        run_job(jobspec)


@pytest.mark.redis_db
def test_generate_query() -> None:
    job = ScrubUserFromEAPSpansStrAttrs(
        JobSpec(
            "bassa",
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, 3, 5, 6],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        )
    )
    assert (
        job._get_query(None)
        == """ALTER TABLE spans_str_attrs_3_local

DELETE WHERE
(
    attr_key = 'sentry.user.ip' OR
    (
        attr_key = 'sentry.user' AND
        startsWith(attr_value, 'ip:') AND
        (isIPv4String(substring(attr_value, 4)) OR isIPv6String(substring(attr_value, 4)))
    )
)
AND organization_id IN [1,3,5,6]
AND timestamp >= toDateTime('2024-12-01T00:00:00')
AND timestamp < toDateTime('2024-12-10T00:00:00')"""
    )


def _clear_attr_storage() -> None:
    from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
    from snuba.clusters.storage_sets import StorageSetKey

    cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
    storage_node = cluster.get_local_nodes()[0]
    connection = cluster.get_node_connection(
        ClickhouseClientSettings.MIGRATE, storage_node
    )
    connection.execute("TRUNCATE TABLE IF EXISTS spans_str_attrs_3_local")
    connection.execute("TRUNCATE TABLE IF EXISTS spans_str_attrs_3_dist")


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_simple_case() -> None:
    # the table is populated via materialized view, therefore may not be dropped
    # by the time we do this test
    _clear_attr_storage()
    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [
        gen_message(tags={"tag1": "herp", "tag2": "herp"}, sentry_tags={}),
        gen_message(
            tags={
                "tag1": "herpderp",
                "tag2": "herp",
            },
            sentry_tags={
                "user": "ip:1.2.3.4",
                "user.ip": "1.2.3.4",
            },
        ),
        gen_message(
            tags={
                "tag1": "durp",
                "tag3": "herp",
            },
            sentry_tags={
                "user": "ip:2.3.4.5",
                "user.ip": "2.3.4.5",
            },
        ),
        gen_message(tags={"tag1": "blah", "tag2": "herp"}, sentry_tags={}),
        gen_message(
            tags={
                "tag1": "derpderp",
                "tag2": "derp",
            },
            sentry_tags={
                "user": "ip:3.4.5.6",
                "user.ip": "3.4.5.6",
            },
        ),
        gen_message(tags={"tag2": "hehe"}, sentry_tags={"user.ip": "5.5.5.5"}),
        gen_message(
            tags={"tag1": "some_last_value"}, sentry_tags={"user": "normal-user"}
        ),
    ]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore

    assert get_attributes("sentry.user").values == [
        "ip:1.2.3.4",
        "ip:2.3.4.5",
        "ip:3.4.5.6",
        "normal-user",
    ]
    assert get_attributes("sentry.user.ip").values == [
        "1.2.3.4",
        "2.3.4.5",
        "3.4.5.6",
        "5.5.5.5",
    ]
    oldtag1 = get_attributes("tag1")
    oldtag2 = get_attributes("tag2")

    run_job(
        JobSpec(
            "bassa",
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, 3, 5, 6],
                "start_datetime": str(START_TS.ToDatetime()),
                "end_datetime": str(END_TS.ToDatetime()),
            },
        )
    )
    sleep(1)  # wait for the job to finish
    assert get_attributes("sentry.user").values == ["normal-user"]
    assert get_attributes("sentry.user.ip").values == []
    assert get_attributes("tag1").values == oldtag1.values
    assert get_attributes("tag2").values == oldtag2.values


def get_attributes(key: str) -> TraceItemAttributeValuesResponse:
    msg = TraceItemAttributeValuesRequest(
        meta=COMMON_META,
        limit=50,
        key=AttributeKey(name=key, type=AttributeKey.TYPE_STRING),
    )
    return AttributeValuesRequest().execute(msg)


BASE_TIME = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
) - timedelta(minutes=180)
START_TS = Timestamp(
    seconds=int(
        datetime(
            year=BASE_TIME.year,
            month=BASE_TIME.month,
            day=BASE_TIME.day,
            tzinfo=UTC,
        ).timestamp()
    )
)
END_TS = Timestamp(
    seconds=int(
        (
            datetime(
                year=BASE_TIME.year,
                month=BASE_TIME.month,
                day=BASE_TIME.day,
                tzinfo=UTC,
            )
            + timedelta(days=1)
        ).timestamp()
    )
)
COMMON_META = RequestMeta(
    project_ids=[1, 2, 3],
    organization_id=1,
    cogs_category="something",
    referrer="something",
    start_timestamp=START_TS,
    end_timestamp=END_TS,
    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
)


def gen_message(
    tags: Mapping[str, str], sentry_tags: Mapping[str, str]
) -> Mapping[str, Any]:
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {},
        "measurements": {},
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": sentry_tags,
        "span_id": uuid.uuid4().hex,
        "tags": tags,
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(BASE_TIME.timestamp() * 1000),
        "start_timestamp_precise": BASE_TIME.timestamp(),
        "end_timestamp_precise": BASE_TIME.timestamp() + 1,
    }
