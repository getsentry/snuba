from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey


class ReplaysDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.REPLAYS)

    @classmethod
    def is_experimental(cls) -> bool:
        return True
