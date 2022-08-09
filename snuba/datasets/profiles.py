from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKeys


class ProfilesDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKeys.PROFILES)
