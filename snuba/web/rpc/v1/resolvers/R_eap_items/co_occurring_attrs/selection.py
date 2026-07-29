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

# Killswitch-style rollout flag: flip on to allow reading v2, flip back to fall in behind
# v1. Enabling it is not sufficient on its own — a request must also be fully inside the
# window v2 has data for, see for_request.
CO_OCCURRING_ATTRS_V2_OPTION = "use_co_occurring_attrs_v2"

# Unix timestamp of the first `date` bucket the v2 tables hold data for. The v2 tables and
# their materialized view were created on 2026-07-29, and the view only appends buckets
# from when it started running, so v2 has nothing before the Monday of that week
# (2026-07-27 00:00 UTC). `date` is bucketed weekly with toMonday(), and the query rounds
# its lower bound down to the previous Monday, so the cutoff has to be a Monday too:
# anything later would make a request starting mid-week round below the cutoff and read
# a bucket that only exists in v1.
#
# A request reaching back before this reads v1 instead, otherwise the attributes that only
# existed in the earlier part of its range would silently disappear from the results.
# There is no backfill planned, so this stays until v1 is retired.
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION = "co_occurring_attrs_v2_start_timestamp"
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT = 1785110400  # 2026-07-27 00:00:00 UTC


def _v2_covers_request_window(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether v2 has data for the whole time range the request asks about.

    The comparison is against the request's *rounded* lower bound rather than the raw start
    timestamp, because that is the bucket the query actually reads: a request starting
    Wednesday reads from the Monday of that week (see
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
    # ToDatetime() returns a naive UTC datetime, so drop the tzinfo to compare like-for-like
    v2_start = datetime.fromtimestamp(start_timestamp, UTC).replace(tzinfo=None)
    return earliest_bucket >= v2_start


def for_request(request: TraceItemAttributeNamesRequest) -> CoOccurringAttrsSource:
    """The co-occurring-attributes source a request should read.

    v2 requires both the rollout flag and that v2 actually has data covering the requested
    range; a request reaching further back transparently falls back to v1, which has the
    full history.
    """
    if not get_option(CO_OCCURRING_ATTRS_V2_OPTION, False):
        return V1
    return V2 if _v2_covers_request_window(request) else V1
