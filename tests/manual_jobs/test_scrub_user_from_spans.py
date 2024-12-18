import pytest

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job
from snuba.manual_jobs.scrub_user_from_spans import ScrubUserFromSentryTags


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
