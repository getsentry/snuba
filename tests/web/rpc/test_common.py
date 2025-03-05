import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import SubscriptableReference
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    attribute_key_to_expression,
    use_eap_items_table,
)
from tests.conftest import SnubaSetConfig


class TestCommon:
    def test_expression_trace_id(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="sentry.trace_id",
            ),
        ) == f.CAST(column("trace_id"), "String", alias="sentry.trace_id_TYPE_STRING")

    def test_timestamp_columns(self) -> None:
        for col in [
            "sentry.timestamp",
            "sentry.start_timestamp",
            "sentry.end_timestamp",
        ]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == f.CAST(
                column(col[len("sentry.") :]), "String", alias=col + "_TYPE_STRING"
            )
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_INT,
                    name=col,
                ),
            ) == f.CAST(column(col[len("sentry.") :]), "Int64", alias=col + "_TYPE_INT")
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_FLOAT,
                    name=col,
                ),
            ) == f.CAST(
                column(col[len("sentry.") :]), "Float64", alias=col + "_TYPE_FLOAT"
            )
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_DOUBLE,
                    name=col,
                ),
            ) == f.CAST(
                column(col[len("sentry.") :]), "Float64", alias=col + "_TYPE_DOUBLE"
            )

    def test_normalized_col(self) -> None:
        for col in [
            "sentry.span_id",
            "sentry.parent_span_id",
            "sentry.segment_id",
            "sentry.service",
        ]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == column(col[len("sentry.") :], alias=col)

    def test_attributes(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_STRING, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_STRING", column=column("attr_str"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_FLOAT, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_FLOAT", column=column("attr_num"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_DOUBLE", column=column("attr_num"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_INT, name="derp"),
        ) == f.CAST(
            SubscriptableReference(
                alias=None,
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Nullable(Int64)",
            alias="derp_TYPE_INT",
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="derp"),
        ) == f.CAST(
            SubscriptableReference(
                alias=None,
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Nullable(Boolean)",
            alias="derp_TYPE_BOOLEAN",
        )

    @pytest.mark.redis_db
    def test_use_eap_items_table(self, snuba_set_config: SnubaSetConfig) -> None:
        snuba_set_config("use_eap_items_table", True)
        snuba_set_config("use_eap_items_table_start_timestamp_seconds", 10)

        assert use_eap_items_table(RequestMeta(start_timestamp=Timestamp(seconds=10)))
        assert not use_eap_items_table(
            RequestMeta(start_timestamp=Timestamp(seconds=9))
        )
