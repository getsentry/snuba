from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.clickhouse.escaping import escape_string
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec


class DeleteEventsByTagKeyValue(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params
        assert isinstance(params["project_ids"], list)
        assert len(params["project_ids"]) > 0
        assert isinstance(params["tag_key"], str)
        assert isinstance(params["tag_value"], str)
        assert params["tag_key"] and params["tag_value"]
        assert all([isinstance(p, int) for p in params["project_ids"]])
        self._project_ids = params["project_ids"]
        self._tag_key = params["tag_key"]
        self._tag_value = params["tag_value"]
        self._start_datetime = datetime.fromisoformat(params["start_datetime"])
        self._end_datetime = datetime.fromisoformat(params["end_datetime"])

    def _get_query(self, cluster_name: str | None) -> str:
        project_ids = ",".join([str(p) for p in self._project_ids])
        key = escape_string(self._tag_key)
        value = escape_string(self._tag_value)
        start_datetime = self._start_datetime.isoformat()
        end_datetime = self._end_datetime.isoformat()
        on_cluster = f"ON CLUSTER '{cluster_name}'" if cluster_name else ""
        return f"""DELETE FROM errors_local {on_cluster}
WHERE project_id IN [{project_ids}]
AND arrayElement(tags.value, indexOf(tags.key, {key})) = {value}
AND timestamp >= toDateTime('{start_datetime}')
AND timestamp < toDateTime('{end_datetime}')"""

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS)
        storage_node = cluster.get_local_nodes()[0]
        connection = cluster.get_node_connection(ClickhouseClientSettings.CLEANUP, storage_node)
        if not cluster.is_single_node():
            cluster_name = cluster.get_clickhouse_cluster_name()
        else:
            cluster_name = None
        query = self._get_query(cluster_name)
        logger.info(f"Executing query: {query}")
        result = connection.execute(
            query=query, settings={"mutations_sync": 0, "lightweight_deletes_sync": 0}
        )

        logger.info("complete")
        logger.info(repr(result))
