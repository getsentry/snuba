from typing import Any, Mapping, Optional, Sequence, Union
import uuid

from snuba.processor import MessageProcessor, ProcessedMessage, ProcessorAction


class QuerylogProcessor(MessageProcessor):
    def __get_request(self, request: Mapping[str, Any]) -> str:
        return str({k: v for k, v in sorted(request.items())})

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
        final = []
        cache_hit = []
        sample = []
        max_threads = []
        duration_ms = []
        trace_id = []

        for query in query_list:
            sql.append(query["sql"])
            status.append(query["status"])
            final.append(query["stats"].get("final", 0))
            cache_hit.append(query["stats"].get("cache_hit", 0))
            sample.append(query["stats"].get("sample", 0))
            max_threads.append(query["stats"].get("max_threads", 0))
            trace_id.append(str(uuid.UUID(query["trace_id"])))
            # TODO: Calculate subquery duration, for now just insert 0s
            duration_ms.append(0)

        return {
            "clickhouse_queries.sql": sql,
            "clickhouse_queries.status": status,
            "clickhouse_queries.final": final,
            "clickhouse_queries.cache_hit": cache_hit,
            "clickhouse_queries.sample": [self.__get_sample(s) for s in sample],
            "clickhouse_queries.max_threads": max_threads,
            "clickhouse_queries.trace_id": trace_id,
            "clickhouse_queries.duration_ms": duration_ms,
        }

    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        action_type = ProcessorAction.INSERT

        projects = message["request"].get("project", [])
        if not isinstance(projects, (list, tuple)):
            projects = [projects]

        processed = {
            "query_id": str(uuid.UUID(message["query_id"])),
            "request": self.__get_request(message["request"]),
            "referrer": message["referrer"],
            "dataset": message["dataset"],
            "projects": projects,
            "organization": None,
            "timestamp": message["timing"]["timestamp"],
            "duration_ms": message["timing"]["duration_ms"],
            "status": message["status"],
            **self.__extract_query_list(message["query_list"]),
        }

        return ProcessedMessage(action=action_type, data=[processed],)
