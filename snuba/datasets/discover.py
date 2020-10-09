from copy import deepcopy
from typing import Any, MutableMapping

from snuba.datasets.dataset import Dataset
from snuba.datasets.entity import Entity
from snuba.datasets.entities.factory import EntityKey, get_entity
from snuba.datasets.entities.discover import (
    detect_table,
    EVENTS_COLUMNS,
    TRANSACTIONS_COLUMNS,
    DiscoverTransactionsEntity,
)
from snuba.query.parser import parse_query_from_entity


# TODO: Clearly this is not the right model, I am just doing this for now
# while I restructure the code, then I can move the entity selection logic back
# out here
class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        self.discover_entity = get_entity(EntityKey.DISCOVER)
        self.discover_transactions_entity = DiscoverTransactionsEntity()
        super().__init__(default_entity=self.discover_entity)

    # XXX: This is temporary code that will eventually need to be ported to Sentry
    # since SnQL will require an entity to always be specified by the user.
    def get_entity(self, query_body: MutableMapping[str, Any]) -> Entity:
        # First parse the query with the default entity
        query_copy = deepcopy(query_body)
        query = parse_query_from_entity(query_copy, self.discover_entity)

        table = detect_table(query, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS, True)

        selected_entity: Entity

        if table == EntityKey.TRANSACTIONS:
            selected_entity = self.discover_transactions_entity
        else:
            selected_entity = self.discover_entity

        self.set_default_entity(selected_entity)

        return selected_entity
