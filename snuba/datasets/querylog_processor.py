import simplejson as json
from typing import Any, Mapping, Optional, Sequence, Union
import uuid

from snuba.processor import MessageProcessor, ProcessedMessage, ProcessorAction


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

        for query in query_list:
            sql.append(query["sql"])
            status.append(query["status"])
            trace_id.append(str(uuid.UUID(query["trace_id"])))
            # TODO: Calculate subquery duration, for now just insert 0s
            duration_ms.append(0)
            stats.append(self.__to_json_string(query["stats"]))


        return {
            "clickhouse_queries.sql": sql,
            "clickhouse_queries.status": status,
            "clickhouse_queries.trace_id": trace_id,
            "clickhouse_queries.duration_ms": duration_ms,
            "clickhouse_queries.stats": stats,
        }

    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        action_type = ProcessorAction.INSERT

        projects = message["request"]["body"].get("project", [])
        if not isinstance(projects, (list, tuple)):
            projects = [projects]

        processed = {
            "request_id": str(uuid.UUID(message["request"]["id"])),
            "request_body": self.__to_json_string(message["request"]["body"]),
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
