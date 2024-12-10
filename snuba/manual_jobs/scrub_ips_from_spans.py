from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec


class ScrubIpFromSentryTags(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params
        assert isinstance(params["project_ids"], list)
        assert [isinstance(p, (int, str)) for p in params["project_ids"]]
        self._project_ids = params["project_ids"]
        self.start_datetime = datetime.fromisoformat(params["start_datetime"])
        self.end_datetime = datetime.fromisoformat(params["end_datetime"])

    def _get_query(self, cluster_name: str) -> str:
        project_ids = ",".join([str(p) for p in self._project_ids])
        start_datetime = self.start_datetime.isoformat()
        end_datetime = self.end_datetime.isoformat()
        return f"""
ALTER TABLE spans_local
ON CLUSTER 'snuba-spans'
UPDATE `sentry_tags.value` = arrayMap((k, v) -> if(k = 'user.ip', 'scrubbed', v), `sentry_tags.key`, `sentry_tags.value`)

WHERE project_id IN [{project_ids}]
AND has(`sentry_tags.key`, 'user.ip')
AND end_timestamp > toDateTime('{start_datetime}')
AND end_timestamp <= toDateTime('{end_datetime}')
    """

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.SPANS)
        connection = cluster.get_query_connection(ClickhouseClientSettings.CLEANUP)
        cluster_name = cluster.get_clickhouse_cluster_name()
        assert cluster_name, "No cluster name for spans storage"
        query = self._get_query(cluster_name)
        logger.info("Executing query: {query}")
        result = connection.execute(query=query, settings={"mutations_sync": 0})
        logger.info("complete")
        logger.info(result.__repr__())
