from collections import OrderedDict

from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeKeyTransformContext,
)

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal, literals_array
from snuba.web.rpc.common import attribute_key_to_expression


class TestCommon:
    def test_project_name_to_expression(self) -> None:
        mapping = OrderedDict()
        mapping[5] = "proj5"
        mapping[1] = "proj1"

        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="project_name",
            ),
            AttributeKeyTransformContext(project_ids_to_names=mapping),
        ) == f.transform(
            column("project_id"),
            literals_array(None, [literal(k) for k in mapping.keys()]),
            literals_array(None, [literal(v) for v in mapping.values()]),
            literal("unknown"),
            alias="project_name",
        )
