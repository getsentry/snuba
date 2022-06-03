from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey


class ExperimentalDataset(Dataset):
    @classmethod
    def is_experimental(cls) -> bool:
        return True

    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.EXPERIMENTAL)
