"""Which co-occurring-attributes source a request reads, and how the v2 rollout is gated."""

from __future__ import annotations

from datetime import UTC, datetime

from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
)

from snuba.state.sentry_options import get_option
from snuba.web.rpc.common.common import prev_monday
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.base import (
    CoOccurringAttrsSource,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.v1 import V1
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.v2 import V2

# Rollout flag. Not sufficient on its own: a request must also fall inside the window v2 has
# data for, see for_request.
CO_OCCURRING_ATTRS_V2_OPTION = "use_co_occurring_attrs_v2"

# First `date` bucket v2 holds data for. Its materialized view was created 2026-07-29 and
# only appends from then on, so v2 has nothing before the Monday of that week. Must stay a
# Monday: `date` is bucketed with toMonday() and queries round down to the previous Monday,
# so a mid-week cutoff would admit requests that then read a bucket only v1 has. No backfill
# is planned, so this stays until v1 is retired.
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION = "co_occurring_attrs_v2_start_timestamp"
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT = 1785110400  # 2026-07-27 00:00:00 UTC


def _v2_covers_request_window(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether v2 has data for the whole time range the request asks about.

    Compares the *rounded* lower bound, since that is the bucket the query actually reads (see
    ``get_co_occurring_attributes_date_condition``). Comparing the raw timestamp would let a
    request starting just after the cutoff read the preceding, non-existent bucket.
    """
    start_timestamp = get_option(
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    )
    earliest_bucket = prev_monday(
        request.meta.start_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)
    )
    # ToDatetime() is naive UTC, so drop the tzinfo to compare like-for-like
    v2_start = datetime.fromtimestamp(start_timestamp, UTC).replace(tzinfo=None)
    return earliest_bucket >= v2_start


def for_request(request: TraceItemAttributeNamesRequest) -> CoOccurringAttrsSource:
    """The source a request should read, falling back to v1 outside v2's data window."""
    if not get_option(CO_OCCURRING_ATTRS_V2_OPTION, False):
        return V1
    return V2 if _v2_covers_request_window(request) else V1
