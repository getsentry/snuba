from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_relay.consts import DataCategory

from snuba.reader import Result


class Outcome:
    ACCEPTED = 0


def num_items_from_outcomes_result(result: Result) -> int:
    data = result.get("data") or []
    if not data:
        return 0
    return int(data[0].get("num_items") or 0)


ITEM_TYPE_TO_OUTCOME_CATEGORY = {
    TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED: DataCategory.DEFAULT,
    TraceItemType.TRACE_ITEM_TYPE_SPAN: DataCategory.SPAN_INDEXED,
    TraceItemType.TRACE_ITEM_TYPE_LOG: DataCategory.LOG_ITEM,
    TraceItemType.TRACE_ITEM_TYPE_METRIC: DataCategory.TRACE_METRIC,
}

ITEM_TYPE_FULL_RETENTION = {
    TraceItemType.TRACE_ITEM_TYPE_UPTIME_RESULT,
    TraceItemType.TRACE_ITEM_TYPE_PREPROD,
    # Occurrences are not outcomes-routed and need exact counts (issue/trace
    # meta), so keep them on tier 1 for the full query window.
    TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
}
