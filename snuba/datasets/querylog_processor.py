import simplejson as json
from typing import Any, Mapping, Optional, Sequence, Union
import uuid

from snuba.processor import MessageProcessor, ProcessedMessage, ProcessorAction


class QuerylogProcessor(MessageProcessor):
    def __get_request(self, request: Mapping[str, Any]) -> str:
        return json.dumps({k: v for k, v in sorted(request.items())})

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
        final = []
        cache_hit = []
        sample = []
        max_threads = []
        duration_ms = []
        num_days = []
        clickhouse_table = []
        query_id = []
        is_duplicate = []

        for query in query_list:
            sql.append(query["sql"])
            status.append(query["status"])
            trace_id.append(str(uuid.UUID(query["trace_id"])))
            # TODO: Calculate subquery duration, for now just insert 0s
            duration_ms.append(0)
            final.append(int(query["stats"].get("final", 0)))
            cache_hit.append(int(query["stats"].get("cache_hit", 0)))
            sample.append(query["stats"].get("sample", 0))
            max_threads.append(query["stats"].get("max_threads", 0))
            num_days.append(query["stats"].get("num_days", 0))
            clickhouse_table.append(query["stats"].get("clickhouse_table", ""))
            query_id.append(query["stats"].get("query_id", 0))
            is_duplicate.append(int(query["stats"].get("is_duplicate", 0)))

        return {
            "clickhouse_queries.sql": sql,
            "clickhouse_queries.status": status,
            "clickhouse_queries.trace_id": trace_id,
            "clickhouse_queries.duration_ms": duration_ms,
            "clickhouse_queries.final": final,
            "clickhouse_queries.cache_hit": cache_hit,
            "clickhouse_queries.sample": [self.__get_sample(s) for s in sample],
            "clickhouse_queries.max_threads": max_threads,
            "clickhouse_queries.num_days": num_days,
            "clickhouse_queries.clickhouse_table": clickhouse_table,
            "clickhouse_queries.query_id": query_id,
            "clickhouse_queries.is_duplicate": is_duplicate,
        }

    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        action_type = ProcessorAction.INSERT

        projects = message["request"]["body"].get("project", [])
        if not isinstance(projects, (list, tuple)):
            projects = [projects]

        processed = {
            "request_id": str(uuid.UUID(message["request"]["id"])),
            "request_body": self.__get_request(message["request"]["body"]),
            "referrer": message["request"]["referrer"] or "",
            "dataset": message["dataset"],
            "projects": projects,
            "organization": None,
            "timestamp": message["timing"]["timestamp"],
            "duration_ms": message["timing"]["duration_ms"],
            "status": message["status"],
            **self.__extract_query_list(message["query_list"]),
        }

        return ProcessedMessage(action=action_type, data=[processed],)
