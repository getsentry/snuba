from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey
from snuba.datasets.entities.discover import (
    detect_table,
    EVENTS_COLUMNS,
    TRANSACTIONS_COLUMNS,
)
from snuba.query.logical import Query


# TODO: Clearly this is not the right model, I am just doing this for now
# while I restructure the code, then I can move the entity selection logic back
# out here
class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        self.discover_entity = EntityKey.DISCOVER
        self.discover_transactions_entity = EntityKey.DISCOVER_TRANSACTIONS
        super().__init__(default_entity=self.discover_entity)

    # XXX: This is temporary code that will eventually need to be ported to Sentry
    # since SnQL will require an entity to always be specified by the user.
    def select_entity(self, query: Query) -> EntityKey:
        table = detect_table(query, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS, True)

        if table == EntityKey.TRANSACTIONS:
            selected_entity = self.discover_transactions_entity
        else:
            selected_entity = self.discover_entity

        return selected_entity
