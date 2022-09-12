from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey


class GroupedMessageDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.GROUPEDMESSAGE)
