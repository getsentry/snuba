import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import SubscriptableReference
from snuba.web.rpc.common.trace_item_types.spans import SpansSnubaRPCBridge


class TestSpansSnubaRPCBridge:
    @pytest.fixture
    def bridge(self) -> SpansSnubaRPCBridge:
        return SpansSnubaRPCBridge()

    def test_expression_trace_id(self, bridge) -> None:
        assert bridge.attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="sentry.trace_id",
            ),
        ) == f.CAST(column("trace_id"), "String", alias="sentry.trace_id_TYPE_STRING")

    def test_timestamp_columns(self, bridge) -> None:
        for col in [
            "sentry.timestamp",
            "sentry.start_timestamp",
            "sentry.end_timestamp",
        ]:
            assert bridge.attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == f.CAST(
                column(col[len("sentry.") :]), "String", alias=col + "_TYPE_STRING"
            )
            assert bridge.attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_INT,
                    name=col,
                ),
            ) == f.CAST(column(col[len("sentry.") :]), "Int64", alias=col + "_TYPE_INT")
            assert bridge.attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_FLOAT,
                    name=col,
                ),
            ) == f.CAST(
                column(col[len("sentry.") :]), "Float64", alias=col + "_TYPE_FLOAT"
            )

    def test_normalized_col(self, bridge) -> None:
        for col in [
            "sentry.span_id",
            "sentry.parent_span_id",
            "sentry.segment_id",
            "sentry.service",
        ]:
            assert bridge.attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == column(col[len("sentry.") :], alias=col)

    def test_attributes(self, bridge) -> None:
        assert bridge.attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_STRING, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_STRING", column=column("attr_str"), key=literal("derp")
        )

        assert bridge.attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_FLOAT, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_FLOAT", column=column("attr_num"), key=literal("derp")
        )

        assert bridge.attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_INT, name="derp"),
        ) == f.CAST(
            SubscriptableReference(
                alias=None,
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Int64",
            alias="derp_TYPE_INT",
        )

        assert bridge.attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="derp"),
        ) == f.CAST(
            SubscriptableReference(
                alias=None,
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Boolean",
            alias="derp_TYPE_BOOLEAN",
        )

    def test_apply_virtual_columns(self, bridge) -> None:
        pass  # TODO write this test
