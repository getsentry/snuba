from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity


class ReplaysDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                get_entity(EntityKey.REPLAYS),
            ]
        )

    @classmethod
    def is_experimental(cls) -> bool:
        return True
