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
    AttributeKey(name="sentry.timestamp", type=AttributeKey.Type.TYPE_STRING),
    AttributeKey(name="sentry.timestamp", type=AttributeKey.Type.TYPE_INT),
    AttributeKey(name="sentry.timestamp", type=AttributeKey.Type.TYPE_FLOAT),
]


@pytest.mark.xfail(reason="want to address this before merging")
@pytest.mark.parametrize("attr_key", tests)
def test_basic(attr_key: AttributeKey) -> None:
    spans_way = attribute_key_to_expression_eap_items(attr_key)
    ourlogs_way = attribute_key_to_expression(attr_key)
    assert spans_way == ourlogs_way
