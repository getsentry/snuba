from copy import deepcopy
from typing import Any, MutableMapping

from snuba.datasets.dataset import Dataset
from snuba.datasets.entity import Entity
from snuba.datasets.entities.factory import EntityKey, get_entity
from snuba.datasets.entities.discover import (
    detect_table,
    EVENTS_COLUMNS,
    TRANSACTIONS_COLUMNS,
)
from snuba.query.parser import _parse_query_impl


# TODO: Clearly this is not the right model, I am just doing this for now
# while I restructure the code, then I can move the entity selection logic back
# out here
class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        self.discover_entity = get_entity(EntityKey.DISCOVER)
        self.transaction_entity = get_entity(EntityKey.TRANSACTIONS)
        super().__init__(default_entity=self.discover_entity)

    # XXX: This is temporary code that will eventually need to be ported to Sentry
    # since SnQL will require an entity to always be specified by the user.
    def get_entity(self, query_body: MutableMapping[str, Any]) -> Entity:
        # First parse the query with the default entity
        query_copy = deepcopy(query_body)
        query = _parse_query_impl(query_copy, self.discover_entity)

        table = detect_table(query, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS, True,)

        if table == EntityKey.TRANSACTIONS:
            return self.transaction_entity
        else:
            return self.discover_entity
