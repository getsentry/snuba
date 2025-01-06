from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec


class ExtractSpanData(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)

    def _generate_spans_query(self):
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

        map_columns = []
        for prefix in ["attr_str_", "attr_num_"]:
            map_columns.extend(f"{prefix}{i}" for i in range(20))

        all_columns = base_columns + map_columns

        scrubbed_columns = []
        for col in all_columns:
            if col in unscrubbed_columns or col.startswith("attr_num"):
                scrubbed_columns.append(col)
            elif col.startswith("attr_str"):
                scrubbed_columns.append(
                    f"mapApply((k, v) -> (k, cityHash64(v)), {col}) AS {col}_scrubbed"
                )
            else:
                scrubbed_columns.append(f"cityHash64({col}) AS {col}_scrubbed")

        query = f"""
        SELECT
            {', '.join(scrubbed_columns)}
        FROM {self.table_name}
        WHERE _sort_timestamp BETWEEN toDateTime('{self.start_timestamp}') AND toDateTime('{self.end_timestamp}')
        AND organization_id IN {self.organization_ids}
        LIMIT {self.limit}
        """

        return query

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
        connection = cluster.get_query_connection(ClickhouseClientSettings.QUERY)

        query = f"""
        INSERT INTO FUNCTION gcs('{self.gcp_bucket_name}/{self.output_file_path}',
            'CSVWithNames',
            'auto',
            'gzip'
        )
        {self._generate_spans_query()}
        """

        logger.info("Executing query")
        connection.execute(query=query)
        logger.info(
            f"Data written to GCS bucket: {self.gcp_bucket_name}/{self.output_file_path}"
        )
