import uuid

import pytest

from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job

_VIEWS = {"spans_num_attrs_3_mv", "spans_str_attrs_3_mv"}


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test() -> None:
    cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
    storage_node = cluster.get_local_nodes()[0]
    connection = cluster.get_node_connection(
        ClickhouseClientSettings.QUERY,
        storage_node,
    )

    for view in _VIEWS:
        connection.execute(f"DROP VIEW {view}")
        with pytest.raises(ClickhouseError):
            connection.execute(f"SELECT * FROM {view}")

    job_id = uuid.uuid4().hex
    run_job(JobSpec(job_id, "RecreateMissingEAPSpansMaterializedViews"))

    assert get_job_status(job_id) == JobStatus.FINISHED

    for view in _VIEWS:
        connection.execute(f"SELECT * FROM {view}")
