"""Pre-aggregated roll-ups of ``eap_items`` that ``TraceItemAttributeNames`` reads from.

``TraceItemAttributeNames`` reads ``eap_item_co_occurring_attrs_v2`` exclusively.
``CoOccurringAttrsV1`` remains as the query shape for the original table, which is
still populated but no longer served.

    v1.py   eap_item_co_occurring_attrs      ReplacingMergeTree, scalar key arrays only
    v2.py   eap_item_co_occurring_attrs_v2   SummingMergeTree, one key array per type
"""

from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.base import (
    CoOccurringAttrsSource,
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
    "CO_OCCURRING_ATTRS_V2_STORAGE_KEY",
    "V1",
    "V2",
    "CoOccurringAttrsSource",
    "CoOccurringAttrsV1",
    "CoOccurringAttrsV2",
]
