from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec

_DICTIONARY_NAME = "default.project_ids_to_scrub"


class ScrubIpFromSentryTagsDictionary(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params
        assert isinstance(params["project_ids"], list)
        assert all([isinstance(p, int) for p in params["project_ids"]])
        self._project_ids = params["project_ids"]
        self._start_datetime = datetime.fromisoformat(params["start_datetime"])
        self._end_datetime = datetime.fromisoformat(params["end_datetime"])
        self._mutations_sync = params.get("mutations_sync", 0)

    def _get_query(self, cluster_name: str | None) -> str:
        start_datetime = self._start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        end_datetime = self._end_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        on_cluster = f"ON CLUSTER '{cluster_name}'" if cluster_name else ""
        return f"""ALTER TABLE spans_local
{on_cluster}
UPDATE `sentry_tags.value` = arrayMap((k, v) -> if(k = 'user.ip', 'scrubbed', v), `sentry_tags.key`, `sentry_tags.value`)
WHERE dictHas('{_DICTIONARY_NAME}', project_id)
AND end_timestamp >= toDateTime('{start_datetime}')
AND end_timestamp < toDateTime('{end_datetime}')"""

    def _dictionary_query(self, cluster_name: str | None) -> str:
        project_ids_str = ",".join([str(p) for p in self._project_ids])
        on_cluster = f"ON CLUSTER '{cluster_name}'" if cluster_name else ""
        return f"""
            CREATE OR REPLACE DICTIONARY {_DICTIONARY_NAME}
            {on_cluster}
            (
                project_id UInt64
            )
            PRIMARY KEY project_id
            SOURCE(
                CLICKHOUSE(
                    QUERY 'SELECT arrayJoin([{project_ids_str}])'
                )
            )
            LIFETIME(MIN 1 MAX 100000)
            LAYOUT(HASHED())
        """

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.SPANS)
        storage_node = cluster.get_local_nodes()[0]
        connection = cluster.get_node_connection(ClickhouseClientSettings.CLEANUP, storage_node)
        if not cluster.is_single_node():
            cluster_name = cluster.get_clickhouse_cluster_name()
        else:
            cluster_name = None
        connection.execute(
            query=self._dictionary_query(cluster_name), settings={"mutations_sync": 2}
        )
        query = self._get_query(cluster_name)
        logger.info(f"Executing query: {query}")
        result = connection.execute(
            query=query,
            settings={
                "mutations_sync": self._mutations_sync,
                "allow_nondeterministic_mutations": True,
            },
        )
        logger.info("complete")
        logger.info(repr(result))
