import uuid
from datetime import datetime, timedelta, timezone

import pytest

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job
from snuba.manual_jobs.scrub_user_from_spans import ScrubUserFromSentryTags
from tests.helpers import write_processed_messages

_IP_ADDRESS = "127.0.0.1"


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_basic() -> None:
    job_id = "abc"
    run_job(
        JobSpec(
            job_id,
            "ScrubUserFromSentryTags",
            False,
            {
                "project_ids": [1, 3, 5, 6],
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
            "ScrubUserFromSentryTags",
            False,
            {
                "project_ids": [1, "b"],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubUserFromSentryTags",
            False,
            {
                "project_ids": [1, 2],
                "start_datetime": "2024-12-01 00:00:0",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubUserFromSentryTags",
            False,
            {
                "project_ids": [1, 2],
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
    job = ScrubUserFromSentryTags(
        JobSpec(
            "bassa",
            "ScrubUserFromSentryTags",
            False,
            {
                "project_ids": [1, 3, 5, 6],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        )
    )
    assert (
        job._get_query(None)
        == """ALTER TABLE spans_local

UPDATE
    `sentry_tags.value` = arrayMap(
        (k, v) -> if(
                k = 'user' AND startsWith(v, 'ip:'),
                concat(
                    'ip:',
                    if(
                        isIPv4String(substring(v, 4)) OR isIPv6String(substring(v, 4)),
                        'scrubbed',
                        substring(v, 4)
                    )
                ),
                v
            ),
        `sentry_tags.key`,
        `sentry_tags.value`
    ),
    `user` = if(
        startsWith(user, 'ip:'),
        concat(
            'ip:',
            if(
                isIPv4String(substring(user, 4)) OR isIPv6String(substring(user, 4)),
                'scrubbed',
                substring(user, 4)
            )
        ),
        user
    )
WHERE project_id IN [1,3,5,6]
AND end_timestamp >= toDateTime('2024-12-01T00:00:00')
AND end_timestamp < toDateTime('2024-12-10T00:00:00')"""
    )

    assert (
        job._get_query("snuba-spans")
        == """ALTER TABLE spans_local
ON CLUSTER 'snuba-spans'
UPDATE
    `sentry_tags.value` = arrayMap(
        (k, v) -> if(
                k = 'user' AND startsWith(v, 'ip:'),
                concat(
                    'ip:',
                    if(
                        isIPv4String(substring(v, 4)) OR isIPv6String(substring(v, 4)),
                        'scrubbed',
                        substring(v, 4)
                    )
                ),
                v
            ),
        `sentry_tags.key`,
        `sentry_tags.value`
    ),
    `user` = if(
        startsWith(user, 'ip:'),
        concat(
            'ip:',
            if(
                isIPv4String(substring(user, 4)) OR isIPv6String(substring(user, 4)),
                'scrubbed',
                substring(user, 4)
            )
        ),
        user
    )
WHERE project_id IN [1,3,5,6]
AND end_timestamp >= toDateTime('2024-12-01T00:00:00')
AND end_timestamp < toDateTime('2024-12-10T00:00:00')"""
    )


def generate_fizzbuzz_events(
    project_ids: list[int], base_time: datetime, minutes: int = 5
) -> None:
    """
    Generate a deterministic set of events across a time range.
    """
    events = []
    storage = get_writable_storage(StorageKey.SPANS)

    trace_id = "7400045b25c443b885914600aa83ad04"
    for tick in range(minutes):
        tock = tick + 1
        for p in project_ids:
            # project N sends an event every Nth minute
            if tock % p == 0:
                span_id = f"8841662216cc598b{tock}"[-16:]
                start_timestamp = base_time + timedelta(minutes=tick)
                duration = timedelta(minutes=tick)
                end_timestamp = start_timestamp + duration
                processed = (
                    storage.get_table_writer()
                    .get_stream_loader()
                    .get_processor()
                    .process_message(
                        {
                            "project_id": p,
                            "organization_id": 1,
                            "event_id": uuid.uuid4().hex,
                            "deleted": 0,
                            "is_segment": False,
                            "duration_ms": int(1000 * duration.total_seconds()),
                            "start_timestamp_ms": int(
                                1000 * start_timestamp.timestamp(),
                            ),
                            "start_timestamp_precise": start_timestamp.timestamp(),
                            "end_timestamp_precise": end_timestamp.timestamp(),
                            "received": start_timestamp.timestamp(),
                            "exclusive_time_ms": int(1000 * 0.1234),
                            "trace_id": trace_id,
                            "span_id": span_id,
                            "retention_days": 30,
                            "parent_span_id": span_id,
                            "description": "GET /api/0/organizations/sentry/tags/?project=1",
                            "measurements": {
                                "lcp": {"value": 32.129},
                                "lcp.elementSize": {"value": 4242},
                            },
                            "breakdowns": {
                                "span_ops": {
                                    "ops.db": {"value": 62.512},
                                    "ops.http": {"value": 109.774},
                                    "total.time": {"value": 172.286},
                                }
                            },
                            "tags": {
                                "sentry:release": str(tick),
                                "sentry:dist": "dist1",
                                # User
                                "foo": "baz",
                                "foo.bar": "qux",
                                "os_name": "linux",
                            },
                            "sentry_tags": {
                                "transaction": "/api/do_things",
                                "transaction.op": "http",
                                "op": "http.client",
                                "status": "unknown",
                                "module": "sentry",
                                "action": "POST",
                                "domain": "sentry.io:1234",
                                "sometag": "somevalue",
                                "user": f"ip:{_IP_ADDRESS}",
                            },
                        },
                        KafkaMessageMetadata(0, 0, base_time),
                    )
                )
                assert processed is not None
                events.append(processed)

    write_processed_messages(storage, events)


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_do_the_thing() -> None:
    # scrub projects 1 and 2, leave 3, 4, 5 alone
    minutes = 5
    base_time = base_time = datetime.now().replace(
        minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    ) - timedelta(minutes=minutes)

    generate_fizzbuzz_events(
        project_ids=list(range(1, 10)), base_time=base_time, minutes=5
    )

    job_id = "abc"
    run_job(
        JobSpec(
            job_id,
            "ScrubUserFromSentryTags",
            False,
            {
                "project_ids": [1, 2],
                "start_datetime": (base_time - timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "end_datetime": (base_time + timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                # only use this in tests
                "mutations_sync": 2,
            },
        )
    )

    assert get_job_status(job_id) == JobStatus.FINISHED
    cluster = get_cluster(StorageSetKey.SPANS)
    storage_node = cluster.get_local_nodes()[0]
    connection = cluster.get_node_connection(
        ClickhouseClientSettings.QUERY, storage_node
    )
    res = connection.execute(
        f"SELECT any(has(sentry_tags.value, '{_IP_ADDRESS}')) FROM spans_local WHERE project_id IN [1,2]"
    )
    assert res.results[0][0] == 0

    res = connection.execute(
        f"SELECT countIf(user = '{_IP_ADDRESS}') FROM spans_local WHERE project_id IN [1,2]"
    )
    assert res.results[0][0] == 0

    res = connection.execute(
        f"SELECT groupBitAnd(has(sentry_tags.value, 'ip:{_IP_ADDRESS}')) FROM spans_local WHERE project_id IN [3, 4, 5]"
    )
    assert res.results[0][0] == 1

    res = connection.execute(
        f"SELECT countIf(user = 'ip:{_IP_ADDRESS}') FROM spans_local WHERE project_id IN [3, 4, 5]"
    )
    assert res.results[0][0] > 0
