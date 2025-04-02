import pdb

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    attribute_key_to_expression_eap_items,
)
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.attribute_key_to_expression import (
    attribute_key_to_expression,
)

# normalized columns are the ones not stores in attr_

tests = [
    AttributeKey(name="sentry.trace_id", type=AttributeKey.Type.TYPE_STRING),
    AttributeKey(name="sentry.item_id", type=AttributeKey.Type.TYPE_STRING),  # fine
    AttributeKey(name="sentry.timestamp", type=AttributeKey.Type.TYPE_STRING),
    AttributeKey(name="sentry.timestamp", type=AttributeKey.Type.TYPE_INT),
    AttributeKey(name="sentry.timestamp", type=AttributeKey.Type.TYPE_FLOAT),
    AttributeKey(
        name="sentry.organization_id", type=AttributeKey.Type.TYPE_INT
    ),  # fine
    AttributeKey(name="sentry.project_id", type=AttributeKey.Type.TYPE_INT),  # fine
    AttributeKey(
        name="kyles_attr", type=AttributeKey.Type.TYPE_STRING
    ),  # should be fine
]


@pytest.mark.parametrize("attr_key", tests)
def test_basic(attr_key: AttributeKey) -> None:
    if attr_key.name in [
        "sentry.item_id",
        "sentry.organization_id",
        "sentry.project_id",
        "kyles_attr",
    ]:
        return
    spans_way = attribute_key_to_expression_eap_items(attr_key)
    ourlogs_way = attribute_key_to_expression(attr_key)
    if spans_way != ourlogs_way:
        print(f"spans_way: {spans_way}")
        print(f"ourlogs_way: {ourlogs_way}")
        pdb.set_trace()
    assert spans_way == ourlogs_way
