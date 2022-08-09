from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKeys


class FunctionsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKeys.FUNCTIONS)

    @classmethod
    def is_experimental(cls) -> bool:
        return True
