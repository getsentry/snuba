from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey


class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                EntityKey.DISCOVER,
                EntityKey.DISCOVER_EVENTS,
                EntityKey.DISCOVER_TRANSACTIONS,
            ]
        )
