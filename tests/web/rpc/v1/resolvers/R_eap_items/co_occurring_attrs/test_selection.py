"""How the v2 rollout is gated: which source a given request is routed to."""

from datetime import UTC, datetime, timedelta

import pytest
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.web.rpc.v1.resolvers.R_eap_items import co_occurring_attrs
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs import V1, V2
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.selection import (
    CO_OCCURRING_ATTRS_V2_OPTION,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
)

V2_START = datetime.fromtimestamp(CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT, UTC)


def _request(
    start: datetime,
    attr_type: AttributeKey.Type.ValueType = AttributeKey.Type.TYPE_STRING,
) -> TraceItemAttributeNamesRequest:
    req = TraceItemAttributeNamesRequest(meta=RequestMeta(project_ids=[1], organization_id=1))
    req.meta.start_timestamp.FromDatetime(start)
    req.meta.end_timestamp.FromDatetime(start + timedelta(hours=1))
    req.type = attr_type
    return req


@pytest.mark.redis_db
class TestForRequest:
    def test_default_inside_window_reads_v2(self) -> None:
        assert co_occurring_attrs.for_request(_request(V2_START)) is V2

    def test_flag_off_reads_v1(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert co_occurring_attrs.for_request(_request(V2_START)) is V1

    def test_flag_off_reads_v1_even_well_inside_the_window(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert co_occurring_attrs.for_request(_request(V2_START + timedelta(days=90))) is V1

    def test_flag_on_inside_window_reads_v2(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: True}):
            assert co_occurring_attrs.for_request(_request(V2_START)) is V2

    def test_flag_on_before_window_falls_back_to_v1(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: True}):
            assert co_occurring_attrs.for_request(_request(V2_START - timedelta(seconds=1))) is V1

    def test_gate_compares_the_rounded_bucket(self) -> None:
        """The gate must compare the bucket the query reads, not the raw start timestamp.

        With a mid-week cutoff, a request starting after that instant still reads from the
        Monday before it — a bucket v2 never populated — so it must fall back to v1.
        """
        wednesday = V2_START + timedelta(days=2)
        with override_options(
            "snuba",
            {
                CO_OCCURRING_ATTRS_V2_OPTION: True,
                CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: int(wednesday.timestamp()),
            },
        ):
            assert co_occurring_attrs.for_request(_request(wednesday + timedelta(hours=1))) is V1

    def test_start_timestamp_option_widens_the_window(self) -> None:
        old = _request(V2_START - timedelta(days=365))
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: True}):
            assert co_occurring_attrs.for_request(old) is V1
        with override_options(
            "snuba",
            {
                CO_OCCURRING_ATTRS_V2_OPTION: True,
                CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: 0,
            },
        ):
            assert co_occurring_attrs.for_request(old) is V2

    def test_default_cutoff_is_a_monday(self) -> None:
        """`date` is bucketed with toMonday() and the query rounds down to the previous
        Monday, so a mid-week cutoff would admit requests that read a v1-only bucket."""
        assert V2_START.weekday() == 0
        assert (V2_START.hour, V2_START.minute, V2_START.second) == (0, 0, 0)
