from datetime import datetime

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec


class ExtractSpanData(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)

    def _generate_spans_query(self):
        # Columns that should not be hashed (numeric and date types)
        numeric_columns = {
            "organization_id",
            "project_id",
            "span_id",
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
            if col in numeric_columns or col.startswith("attr_num"):
                scrubbed_columns.append(col)
            elif col.startswith("attr_str"):
                scrubbed_columns.append(
                    f"mapApply((k, v) -> (k, cityHash64(v)), {col}) AS {col}"
                )
            else:
                scrubbed_columns.append(f"cityHash64({col}) AS {col}")

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

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"scrubbed_spans_data_{current_time}.csv.gz"

        query = f"""
        INSERT INTO FUNCTION gcs('https://storage.googleapis.com/{self.gcp_bucket_name}/{file_name}',
            'CSVWithNames',
            '',
            'gzip'
        )
        {self._generate_spans_query()}
        """

        logger.info("Executing query")
        connection.execute(query=query)
        logger.info(
            f"Data written to GCS bucket: https://storage.googleapis.com/{self.gcp_bucket_name}/{file_name}"
        )
