#!/usr/bin/env python3

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec

materialized_views = [
    "CREATE MATERIALIZED VIEW IF NOT EXISTS default.spans_num_attrs_3_mv TO default.spans_num_attrs_3_local (`organization_id` UInt64, `project_id` UInt64, `attr_key` String CODEC(ZSTD(1)), `attr_min_value` SimpleAggregateFunction(min, Float64), `attr_max_value` SimpleAggregateFunction(max, Float64), `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1)), `retention_days` UInt16, `count` SimpleAggregateFunction(sum, UInt64)) AS SELECT organization_id, project_id, attrs.1 AS attr_key, attrs.2 AS attr_min_value, attrs.2 AS attr_max_value, toStartOfDay(_sort_timestamp) AS timestamp, retention_days, 1 AS count FROM default.eap_spans_2_local LEFT ARRAY JOIN arrayConcat(CAST(attr_num_0, 'Array(Tuple(String, Float64))'), CAST(attr_num_1, 'Array(Tuple(String, Float64))'), CAST(attr_num_2, 'Array(Tuple(String, Float64))'), CAST(attr_num_3, 'Array(Tuple(String, Float64))'), CAST(attr_num_4, 'Array(Tuple(String, Float64))'), CAST(attr_num_5, 'Array(Tuple(String, Float64))'), CAST(attr_num_6, 'Array(Tuple(String, Float64))'), CAST(attr_num_7, 'Array(Tuple(String, Float64))'), CAST(attr_num_8, 'Array(Tuple(String, Float64))'), CAST(attr_num_9, 'Array(Tuple(String, Float64))'), CAST(attr_num_10, 'Array(Tuple(String, Float64))'), CAST(attr_num_11, 'Array(Tuple(String, Float64))'), CAST(attr_num_12, 'Array(Tuple(String, Float64))'), CAST(attr_num_13, 'Array(Tuple(String, Float64))'), CAST(attr_num_14, 'Array(Tuple(String, Float64))'), CAST(attr_num_15, 'Array(Tuple(String, Float64))'), CAST(attr_num_16, 'Array(Tuple(String, Float64))'), CAST(attr_num_17, 'Array(Tuple(String, Float64))'), CAST(attr_num_18, 'Array(Tuple(String, Float64))'), CAST(attr_num_19, 'Array(Tuple(String, Float64))'), [('sentry.duration_ms', duration_micro / 1000)]) AS attrs GROUP BY organization_id, project_id, attrs.1, attrs.2, timestamp, retention_days",
    "CREATE MATERIALIZED VIEW IF NOT EXISTS default.spans_str_attrs_3_mv TO default.spans_str_attrs_3_local (`organization_id` UInt64, `project_id` UInt64, `attr_key` String CODEC(ZSTD(1)), `attr_value` String CODEC(ZSTD(1)), `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1)), `retention_days` UInt16, `count` SimpleAggregateFunction(sum, UInt64)) AS SELECT organization_id, project_id, attrs.1 AS attr_key, attrs.2 AS attr_value, toStartOfDay(_sort_timestamp) AS timestamp, retention_days, 1 AS count FROM default.eap_spans_2_local LEFT ARRAY JOIN arrayConcat(CAST(attr_str_0, 'Array(Tuple(String, String))'), CAST(attr_str_1, 'Array(Tuple(String, String))'), CAST(attr_str_2, 'Array(Tuple(String, String))'), CAST(attr_str_3, 'Array(Tuple(String, String))'), CAST(attr_str_4, 'Array(Tuple(String, String))'), CAST(attr_str_5, 'Array(Tuple(String, String))'), CAST(attr_str_6, 'Array(Tuple(String, String))'), CAST(attr_str_7, 'Array(Tuple(String, String))'), CAST(attr_str_8, 'Array(Tuple(String, String))'), CAST(attr_str_9, 'Array(Tuple(String, String))'), CAST(attr_str_10, 'Array(Tuple(String, String))'), CAST(attr_str_11, 'Array(Tuple(String, String))'), CAST(attr_str_12, 'Array(Tuple(String, String))'), CAST(attr_str_13, 'Array(Tuple(String, String))'), CAST(attr_str_14, 'Array(Tuple(String, String))'), CAST(attr_str_15, 'Array(Tuple(String, String))'), CAST(attr_str_16, 'Array(Tuple(String, String))'), CAST(attr_str_17, 'Array(Tuple(String, String))'), CAST(attr_str_18, 'Array(Tuple(String, String))'), CAST(attr_str_19, 'Array(Tuple(String, String))'), [('sentry.service', service), ('sentry.segment_name', segment_name), ('sentry.name', name)]) AS attrs GROUP BY organization_id, project_id, attr_key, attr_value, timestamp, retention_days",
]


class RecreateMissingEAPSpansMaterializedViews(Job):
    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)

        for storage_node in cluster.get_local_nodes():
            connection = cluster.get_node_connection(
                ClickhouseClientSettings.CLEANUP,
                storage_node,
            )
            for query in materialized_views:
                logger.info("Executing query: {query}")
                connection.execute(query=query)

        logger.info("complete")
