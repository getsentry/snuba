from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKeys


class ReplaysDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKeys.REPLAYS)

    @classmethod
    def is_experimental(cls) -> bool:
        return True
