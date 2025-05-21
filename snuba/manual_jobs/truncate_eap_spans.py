#!/usr/bin/env python3

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec

STATEMENT = (
    "TRUNCATE TABLE eap_spans_2_local ON CLUSTER 'snuba-events-analytics-platform'"
)


class TruncateEAPSpans(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)

        storage_node = cluster.get_local_nodes()[0]
        connection = cluster.get_node_connection(
            ClickhouseClientSettings.MIGRATE,
            storage_node,
        )
        logger.info(f"Run truncate table statement: {STATEMENT}")
        connection.execute(query=STATEMENT)

        logger.info("TRUNCATE completed")
