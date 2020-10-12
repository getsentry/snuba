from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey

# TODO: Clearly this is not the right model, I am just doing this for now
# while I restructure the code, then I can move the entity selection logic back
# out here
class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.DISCOVER)
