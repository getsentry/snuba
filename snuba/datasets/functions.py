from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey


class FunctionsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                EntityKey.FUNCTIONS,
            ]
        )

    @classmethod
    def is_experimental(cls) -> bool:
        return True
