from __future__ import annotations

import pytest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.datasets.deletion_settings import get_trace_item_type_name


def test_get_trace_item_type_name_valid() -> None:
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED) == "unspecified"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_SPAN) == "span"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_ERROR) == "error"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_LOG) == "log"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK) == "uptime_check"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_UPTIME_RESULT) == "uptime_result"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_REPLAY) == "replay"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE) == "occurrence"
    assert get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_METRIC) == "metric"
    assert (
        get_trace_item_type_name(TraceItemType.TRACE_ITEM_TYPE_PROFILE_FUNCTION)
        == "profile_function"
    )


def test_get_trace_item_type_name_by_integer() -> None:
    assert get_trace_item_type_name(0) == "unspecified"
    assert get_trace_item_type_name(1) == "span"
    assert get_trace_item_type_name(7) == "occurrence"


def test_get_trace_item_type_name_invalid() -> None:
    with pytest.raises(ValueError, match="Unknown TraceItemType value: 999"):
        get_trace_item_type_name(999)

    with pytest.raises(ValueError, match="Unknown TraceItemType value: -1"):
        get_trace_item_type_name(-1)
