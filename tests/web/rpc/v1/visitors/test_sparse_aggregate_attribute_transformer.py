from datetime import UTC, datetime

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    ExtrapolationMode,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ExistsFilter,
    OrFilter,
    TraceItemFilter,
)

from snuba.web.rpc.v1.visitors.sparse_aggregate_attribute_transformer import (
    SparseAggregateAttributeTransformer,
)


def test_basic() -> None:
    ts = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    req = TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=int(ts.timestamp())),
            end_timestamp=Timestamp(seconds=int(ts.timestamp())),
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        filter=TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.category")
            )
        ),
        columns=[
            Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
            Column(
                conditional_aggregation=AttributeConditionalAggregation(
                    aggregate=Function.FUNCTION_MAX,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                    ),
                    label="max(my.float.field)",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ),
            Column(
                conditional_aggregation=AttributeConditionalAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                    ),
                    label="avg(my.float.field)",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ),
        ],
        group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
        order_by=[
            TraceItemTableRequest.OrderBy(
                column=Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                )
            ),
        ],
        limit=5,
    )
    transformed = SparseAggregateAttributeTransformer(req).transform()
    # filter was set properly
    print(transformed.filter)
    assert transformed.filter == TraceItemFilter(
        and_filter=AndFilter(
            filters=[
                TraceItemFilter(
                    exists_filter=ExistsFilter(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.category"
                        )
                    )
                ),
                TraceItemFilter(
                    or_filter=OrFilter(
                        filters=[
                            TraceItemFilter(
                                exists_filter=ExistsFilter(
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE,
                                        name="my.float.field",
                                    )
                                )
                            ),
                            TraceItemFilter(
                                exists_filter=ExistsFilter(
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE,
                                        name="my.float.field",
                                    )
                                )
                            ),
                        ]
                    )
                ),
            ]
        )
    )
    # nothing else changed
    transformed.filter.CopyFrom(req.filter)
    assert transformed == req


def test_no_aggregate() -> None:
    ts = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    req = TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=int(ts.timestamp())),
            end_timestamp=Timestamp(seconds=int(ts.timestamp())),
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        filter=TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.category")
            )
        ),
        columns=[
            Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
        ],
        order_by=[
            TraceItemTableRequest.OrderBy(
                column=Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                )
            ),
        ],
        limit=5,
    )
    transformed = SparseAggregateAttributeTransformer(req).transform()
    assert transformed == req
