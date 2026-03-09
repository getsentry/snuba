from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_relay.consts import DataCategory


class Outcome:
    ACCEPTED = 0


ITEM_TYPE_TO_OUTCOME_CATEGORY = {
    TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED: DataCategory.DEFAULT,
    TraceItemType.TRACE_ITEM_TYPE_SPAN: DataCategory.SPAN_INDEXED,
    TraceItemType.TRACE_ITEM_TYPE_LOG: DataCategory.LOG_ITEM,
    TraceItemType.TRACE_ITEM_TYPE_METRIC: DataCategory.TRACE_METRIC,
}

ITEM_TYPE_FULL_RETENTION = {
    TraceItemType.TRACE_ITEM_TYPE_UPTIME_RESULT,
    TraceItemType.TRACE_ITEM_TYPE_PREPROD,
}
