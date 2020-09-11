from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity

# TODO: Clearly this is not the right model, I am just doing this for now
# while I restructure the code, then I can move the entity selection logic back
# out here
class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        discover_entity = get_entity(EntityKey.DISCOVER)
        super().__init__(entities=[discover_entity])
