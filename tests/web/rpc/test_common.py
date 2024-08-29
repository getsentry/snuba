from collections import OrderedDict

from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeKeyTransformContext,
)

from snuba.query import SubscriptableReference
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal, literals_array
from snuba.web.rpc.common import attribute_key_to_expression


class TestCommon:
    def test_expression_project_name(self) -> None:
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

    def test_expression_trace_id(self):
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="trace_id",
            ),
            AttributeKeyTransformContext(),
        ) == f.CAST(column("trace_id"), "String", alias="trace_id")

    def test_hex_id_columns(self):
        for col in ["span_id", "parent_span_id", "segment_id"]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
                AttributeKeyTransformContext(),
            ) == f.hex(column(col), alias=col)

            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_INT,
                    name=col,
                ),
                AttributeKeyTransformContext(),
            ) == f.CAST(column(col), "UInt64", alias=col)

    def test_timestamp_columns(self):
        for col in ["timestamp", "start_timestamp", "end_timestamp"]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
                AttributeKeyTransformContext(),
            ) == f.CAST(column(col), "String", alias=col)
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_INT,
                    name=col,
                ),
                AttributeKeyTransformContext(),
            ) == f.CAST(column(col), "Int64", alias=col)
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_FLOAT,
                    name=col,
                ),
                AttributeKeyTransformContext(),
            ) == f.CAST(column(col), "Float64", alias=col)

    def test_normalized_col(self):
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="service",
            ),
            AttributeKeyTransformContext(),
        ) == column("service")

    def test_attributes(self):
        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_STRING, name="derp"),
            AttributeKeyTransformContext(),
        ) == SubscriptableReference(
            alias="derp", column=column("attr_str"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_FLOAT, name="derp"),
            AttributeKeyTransformContext(),
        ) == SubscriptableReference(
            alias="derp", column=column("attr_num"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_INT, name="derp"),
            AttributeKeyTransformContext(),
        ) == f.CAST(
            SubscriptableReference(
                alias="derp",
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Int64",
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="derp"),
            AttributeKeyTransformContext(),
        ) == f.CAST(
            SubscriptableReference(
                alias="derp",
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Boolean",
        )
