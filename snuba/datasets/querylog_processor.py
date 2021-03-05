import uuid
from typing import Any, Mapping, Optional, Sequence, Union

import simplejson as json

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper


metrics = MetricsWrapper(environment.metrics, "snuba.querylog")


class QuerylogProcessor(MessageProcessor):
    def __to_json_string(self, map: Mapping[str, Any]) -> str:
        return json.dumps({k: v for k, v in sorted(map.items())})

    def __get_sample(self, sample: Union[int, float]) -> float:
        """
        Since sample can be both integer or float between 0 and 1, we just cast
        int to float so we have a common data type.
        """
        if isinstance(sample, int):
            return float(sample)

        return sample

    def __extract_query_list(
        self, query_list: Sequence[Mapping[str, Any]]
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

        for query in query_list:
            sql.append(query["sql"])
            status.append(query["status"])
            trace_id.append(str(uuid.UUID(query["trace_id"])))
            # TODO: Calculate subquery duration, for now just insert 0s
            duration_ms.append(0)
            stats.append(self.__to_json_string(query["stats"]))
            final.append(int(query["stats"].get("final") or 0))
            cache_hit.append(int(query["stats"].get("cache_hit") or 0))
            sample.append(query["stats"].get("sample") or 0)
            max_threads.append(query["stats"].get("max_threads") or 0)
            clickhouse_table.append(query["stats"].get("clickhouse_table") or "")
            query_id.append(query["stats"].get("query_id") or "")
            # XXX: ``is_duplicate`` is currently not set when using the
            # ``Cache.get_readthrough`` query execution path. See GH-902.
            is_duplicate.append(int(query["stats"].get("is_duplicate") or 0))
            consistent.append(int(query["stats"].get("consistent") or 0))
            profile = query.get("profile") or {
                "time_range": 0,
                "all_columns": [],
                "multi_level_condition": False,
                "where_profile": {"columns": [], "mapping_cols": []},
                "groupby_cols": [],
                "array_join_cols": [],
            }
            time_range = profile["time_range"]
            num_days.append(
                time_range if time_range is not None and time_range >= 0 else 0
            )
            all_columns.append(profile.get("all_columns") or [])
            or_conditions.append(profile["multi_level_condition"])
            where_columns.append(profile["where_profile"]["columns"])
            where_mapping_columns.append(profile["where_profile"]["mapping_cols"])
            groupby_columns.append(profile["groupby_cols"])
            array_join_columns.append(profile["array_join_cols"])

        return {
            "clickhouse_queries.sql": sql,
            "clickhouse_queries.status": status,
            "clickhouse_queries.trace_id": trace_id,
            "clickhouse_queries.duration_ms": duration_ms,
            "clickhouse_queries.stats": stats,
            "clickhouse_queries.final": final,
            "clickhouse_queries.cache_hit": cache_hit,
            "clickhouse_queries.sample": [self.__get_sample(s) for s in sample],
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
        }

    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        projects = message["request"]["body"].get("project", [])
        if not isinstance(projects, (list, tuple)):
            projects = [projects]

        processed = {
            "request_id": str(uuid.UUID(message["request"]["id"])),
            "request_body": self.__to_json_string(message["request"]["body"]),
            "referrer": message["request"]["referrer"] or "",
            "dataset": message["dataset"],
            "projects": projects,
            # TODO: This column is empty for now, we plan to use it soon as we
            # will start to write org IDs into events and allow querying by org.
            "organization": None,
            **self.__extract_query_list(message["query_list"]),
        }

        # These fields are sometimes missing from the payload. If they are missing, don't
        # add them to processed so Clickhouse sets a default value for them.
        missing_fields = {}
        timing = message.get("timing") or {}
        if timing.get("timestamp") is not None:
            missing_fields["timestamp"] = timing["timestamp"]
        if timing.get("duration_ms") is not None:
            missing_fields["duration_ms"] = timing["duration_ms"]
        if message.get("status") is not None:
            missing_fields["status"] = message["status"]

        missing_keys = set(["timestamp", "duration_ms", "status"])
        for key, val in missing_fields.items():
            if key in processed:
                missing_keys.remove(key)
            elif val is not None:
                processed[key] = val
                missing_keys.remove(key)

        if missing_keys:
            metrics.increment(
                "process.missing_fields",
                tags={"fields": ",".join(sorted(missing_keys))},
            )
        return InsertBatch([processed], None)
