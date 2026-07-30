"""Pre-aggregated roll-ups of ``eap_items`` that ``TraceItemAttributeNames`` reads from.

The two roll-ups have different schemas, so choosing one also decides part of the query's
shape. ``CoOccurringAttrsSource`` is that choice, one implementation per table:

    v1.py   eap_item_co_occurring_attrs      ReplacingMergeTree, scalar key arrays only
    v2.py   eap_item_co_occurring_attrs_v2   SummingMergeTree, one key array per type

``for_request`` picks the source for a request.
"""

from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.base import (
    CoOccurringAttrsSource,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.selection import (
    CO_OCCURRING_ATTRS_V2_OPTION,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
    for_request,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.v1 import (
    CO_OCCURRING_ATTRS_STORAGE_KEY,
    V1,
    CoOccurringAttrsV1,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.v2 import (
    CO_OCCURRING_ATTRS_V2_STORAGE_KEY,
    V2,
    CoOccurringAttrsV2,
)

__all__ = [
    "CO_OCCURRING_ATTRS_STORAGE_KEY",
    "CO_OCCURRING_ATTRS_V2_OPTION",
    "CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT",
    "CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION",
    "CO_OCCURRING_ATTRS_V2_STORAGE_KEY",
    "V1",
    "V2",
    "CoOccurringAttrsSource",
    "CoOccurringAttrsV1",
    "CoOccurringAttrsV2",
    "for_request",
]
