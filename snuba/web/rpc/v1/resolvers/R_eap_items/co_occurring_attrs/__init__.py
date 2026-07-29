"""The co-occurring-attributes roll-ups that ``TraceItemAttributeNames`` reads from.

The endpoint answers "which attribute keys co-occur with these ones" from a pre-aggregated
roll-up of ``eap_items`` rather than from the item table itself. There are two such
roll-ups and they do not have the same schema, so choosing one also decides part of the
query's shape. ``CoOccurringAttrsSource`` is that choice: one implementation per table,
each owning the query shape its own schema requires.

    v1.py   eap_item_co_occurring_attrs      ReplacingMergeTree, scalar key arrays only
    v2.py   eap_item_co_occurring_attrs_v2   SummingMergeTree, one key array per type

Use ``for_request`` to get the source a request should read; see ``selection.py`` for how
the rollout is gated.

This deliberately sits above the entity layer rather than in a ``QueryStorageSelector``. A
selector picks a *table* for an otherwise-identical query, whereas these two need different
SELECT lists and a different aggregate. The selector would also never run here: the
endpoint builds a ``Storage`` data source, so ``EntityProcessingStage`` returns via
``try_translate_storage_query`` before ``select_storage`` is reached.
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
