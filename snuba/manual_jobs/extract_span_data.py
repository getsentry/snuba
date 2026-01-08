from typing import Any, Mapping, Optional

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec


class ExtractSpanData(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params
        required_params = [
            "organization_ids",
            "start_timestamp",
            "end_timestamp",
            "table_name",
            "limit",
            "gcp_bucket_name",
            "output_file_path",
            "allowed_keys",
        ]
        for param in required_params:
            assert param in params

        self._organization_ids = params["organization_ids"]
        self._start_timestamp = params["start_timestamp"]
        self._end_timestamp = params["end_timestamp"]
        self._table_name = params["table_name"]
        self._limit = params["limit"]
        self._gcp_bucket_name = params["gcp_bucket_name"]
        self._output_file_path = params["output_file_path"]
        self._allowed_keys = params["allowed_keys"]

    def _generate_spans_query(self) -> str:
        # Columns that should not be scrubbed
        unscrubbed_columns = {
            "span_id",
            "trace_id",
            "parent_span_id",
            "segment_id",
            "is_segment",
            "_sort_timestamp",
            "start_timestamp",
            "end_timestamp",
            "duration_micro",
            "exclusive_time_micro",
            "retention_days",
            "sampling_factor",
            "sampling_weight",
            "sign",
        }

        base_columns = [
            "organization_id",
            "project_id",
            "service",
            "trace_id",
            "span_id",
            "parent_span_id",
            "segment_id",
            "segment_name",
            "is_segment",
            "_sort_timestamp",
            "start_timestamp",
            "end_timestamp",
            "duration_micro",
            "exclusive_time_micro",
            "retention_days",
            "name",
            "sampling_factor",
            "sampling_weight",
            "sign",
        ]

        map_columns: list[str] = []
        for prefix in ["attr_str_", "attr_num_"]:
            map_columns.extend(f"{prefix}{i}" for i in range(20))

        all_columns = base_columns + map_columns

        # We scrub all strings except for the allowed keys.
        # To perform the scrubbing, we generate a salt based on the orgnization_id using sipHash128Reference (we use a different hash function for the salt so that we don't end up storing the salt).
        # We then concatenate the salt with the value we are hashing and hash the result with BLAKE3.
        scrubbed_columns = []
        for col in all_columns:
            if col in unscrubbed_columns:
                scrubbed_columns.append(col)
            elif col.startswith("attr_num"):
                scrubbed_columns.append(
                    f"mapApply((k, v) -> (if(k in {self._allowed_keys}, k, BLAKE3(concat(sipHash128Reference(organization_id), k))), v), {col}) AS {col}_scrubbed"
                )
            elif col.startswith("attr_str"):
                scrubbed_columns.append(
                    f"mapApply((k, v) -> (if(k in {self._allowed_keys}, k, BLAKE3(concat(sipHash128Reference(organization_id), k))), BLAKE3(concat(sipHash128Reference(organization_id), v))), {col}) AS {col}_scrubbed"
                )
            else:
                scrubbed_columns.append(f"BLAKE3(concat(salt, {col})) AS {col}_scrubbed")

        query = f"""
        SELECT
            {', '.join(scrubbed_columns)}
        FROM {self._table_name}
        WHERE _sort_timestamp BETWEEN toDateTime('{self._start_timestamp}') AND toDateTime('{self._end_timestamp}')
        AND organization_id IN {self._organization_ids}
        LIMIT {self._limit}
        """

        return query

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
        connection = cluster.get_query_connection(ClickhouseClientSettings.QUERY)

        query = f"""
        INSERT INTO FUNCTION gcs('{self._gcp_bucket_name}/{self._output_file_path}',
            'CSVWithNames',
            'auto',
            'gzip'
        )
        {self._generate_spans_query()}
        """

        logger.info("Executing query")
        connection.execute(query=query)
        logger.info(f"Data written to GCS bucket: {self._gcp_bucket_name}/{self._output_file_path}")
