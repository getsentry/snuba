from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec


class ScrubIpFromEAPSpans(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params
        assert isinstance(params["organization_ids"], list)
        assert all([isinstance(p, int) for p in params["organization_ids"]])
        self._organization_ids = params["organization_ids"]
        self._start_datetime = datetime.fromisoformat(params["start_datetime"])
        self._end_datetime = datetime.fromisoformat(params["end_datetime"])

    def _get_query(self, cluster_name: str | None) -> str:
        organization_ids = ",".join([str(p) for p in self._organization_ids])
        start_datetime = self._start_datetime.isoformat()
        end_datetime = self._end_datetime.isoformat()
        on_cluster = f"ON CLUSTER '{cluster_name}'" if cluster_name else ""
        return f"""ALTER TABLE eap_spans_2_local
{on_cluster}
UPDATE `attr_str_14` = mapApply((k, v) -> (k, if(k = 'sentry.user.ip', 'scrubbed', v)), `attr_str_14`)
WHERE organization_id IN [{organization_ids}]
AND _sort_timestamp >= toDateTime('{start_datetime}')
AND _sort_timestamp < toDateTime('{end_datetime}')"""

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
        storage_node = cluster.get_local_nodes()[0]
        connection = cluster.get_node_connection(
            ClickhouseClientSettings.CLEANUP, storage_node
        )
        if not cluster.is_single_node():
            cluster_name = cluster.get_clickhouse_cluster_name()
        else:
            cluster_name = None
        query = self._get_query(cluster_name)
        logger.info("Executing query: {query}")
        result = connection.execute(query=query, settings={"mutations_sync": 0})

        logger.info("complete")
        logger.info(repr(result))
