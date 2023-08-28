from __future__ import annotations

import logging
import uuid
from typing import Any, Mapping, Optional, Sequence, Union

import simplejson as json
from sentry_kafka_schemas.schema_types.snuba_queries_v1 import Querylog, QueryMetadata

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "snuba.querylog")


class QuerylogProcessor(DatasetMessageProcessor):
    def __to_json_string(self, map: Mapping[str, Any]) -> str:
        return json.dumps({k: v for k, v in sorted(map.items())}, separators=(",", ":"))

    def __get_sample(self, sample: Union[int, float]) -> float:
        """
        Since sample can be both integer or float between 0 and 1, we just cast
        int to float so we have a common data type.
        """
        if isinstance(sample, int):
            return float(sample)

        return sample

    def __extract_query_list(
        self, query_list: Sequence[QueryMetadata]
    ) -> Mapping[str, Any]:
        sql = []
        status = []
        trace_id = []
        duration_ms = []
        stats = []
        final = []
        cache_hit = []
        sample = []
        max_threads = []
        num_days = []
        clickhouse_table = []
        query_id = []
        is_duplicate = []
        consistent = []
        all_columns = []
        or_conditions = []
        where_columns = []
        where_mapping_columns = []
        groupby_columns = []
        array_join_columns = []
        bytes_scanned_columns = []

        for query in query_list:
            if "sample" in query["stats"]:
                query["stats"]["sample"] = self.__get_sample(query["stats"]["sample"])
            sql.append(query["sql"])
            status.append(query["status"])
            trace_id.append(str(uuid.UUID(query["trace_id"])))
            stats.append(self.__to_json_string(query["stats"]))
            final.append(int(query["stats"].get("final") or 0))
            cache_hit.append(int(query["stats"].get("cache_hit", 0) or 0))
            sample.append(float(query["stats"].get("sample", 0.0) or 0.0))
            max_threads.append(query["stats"].get("max_threads") or 0)
            clickhouse_table.append(query["stats"].get("clickhouse_table") or "")
            query_id.append(query["stats"].get("query_id") or "")
            # XXX: ``is_duplicate`` is currently not set when using the
            # ``Cache.get_readthrough`` query execution path. See GH-902.
            is_duplicate.append(int(query["stats"].get("is_duplicate") or 0))
            consistent.append(query["stats"].get("consistent", False) or False)
            profile = query["profile"]
            result_profile = query.get("result_profile") or {"bytes": 0}
            time_range = profile["time_range"]
            num_days.append(
                time_range if time_range is not None and time_range >= 0 else 0
            )
            all_columns.append(profile["all_columns"])
            or_conditions.append(int(profile["multi_level_condition"]))
            where_columns.append(profile["where_profile"]["columns"])
            where_mapping_columns.append(profile["where_profile"]["mapping_cols"])
            groupby_columns.append(profile["groupby_cols"])
            array_join_columns.append(profile["array_join_cols"])
            bytes_scanned_columns.append(result_profile.get("bytes", 0))
            duration_ms.append(int(result_profile.get("elapsed", 0) * 1000))

        return {
            "clickhouse_queries.sql": sql,
            "clickhouse_queries.status": status,
            "clickhouse_queries.trace_id": trace_id,
            "clickhouse_queries.duration_ms": duration_ms,
            "clickhouse_queries.stats": stats,
            "clickhouse_queries.final": final,
            "clickhouse_queries.cache_hit": cache_hit,
            "clickhouse_queries.sample": sample,
            "clickhouse_queries.max_threads": max_threads,
            "clickhouse_queries.num_days": num_days,
            "clickhouse_queries.clickhouse_table": clickhouse_table,
            "clickhouse_queries.query_id": query_id,
            "clickhouse_queries.is_duplicate": is_duplicate,
            "clickhouse_queries.consistent": consistent,
            "clickhouse_queries.all_columns": all_columns,
            "clickhouse_queries.or_conditions": or_conditions,
            "clickhouse_queries.where_columns": where_columns,
            "clickhouse_queries.where_mapping_columns": where_mapping_columns,
            "clickhouse_queries.groupby_columns": groupby_columns,
            "clickhouse_queries.array_join_columns": array_join_columns,
            "clickhouse_queries.bytes_scanned": bytes_scanned_columns,
        }

    def _remove_invalid_data(self, processed: dict[str, Any]) -> None:
        valid_project_ids = []
        # Ignore negative project IDs
        for pid in processed["projects"]:
            try:
                p = int(pid)
            except ValueError:
                logger.error(
                    "Invalid project id",
                    extra=processed,
                    exc_info=True,
                )
                continue

            if p <= 0:
                logger.error(
                    "Invalid project id",
                    extra=processed,
                    exc_info=True,
                )
                continue
            valid_project_ids.append(pid)
        processed["projects"] = valid_project_ids

    def process_message(
        self, message: Querylog, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        processed = {
            "request_id": str(uuid.UUID(message["request"]["id"])),
            "request_body": self.__to_json_string(message["request"]["body"]),
            "referrer": message["request"]["referrer"] or "",
            "dataset": message["dataset"],
            "projects": message.get("projects") or [],
            "organization": message.get("organization"),
            "status": message["status"],
            "timestamp": message["timing"]["timestamp"],
            "duration_ms": message["timing"]["duration_ms"],
            **self.__extract_query_list(message["query_list"]),
        }
        self._remove_invalid_data(processed)

        return InsertBatch([processed], None)
