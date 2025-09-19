from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType


class OutcomeCategory:
    SPAN_INDEXED = 16
    LOG_ITEM = 23


class Outcome:
    ACCEPTED = 0


ITEM_TYPE_TO_OUTCOME_CATEGORY = {
    TraceItemType.TRACE_ITEM_TYPE_SPAN: OutcomeCategory.SPAN_INDEXED,
    TraceItemType.TRACE_ITEM_TYPE_LOG: OutcomeCategory.LOG_ITEM,
}
