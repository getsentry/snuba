from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey


class ErrorsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.ERRORS)
