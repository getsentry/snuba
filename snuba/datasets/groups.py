from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey, get_entity


class Groups(Dataset):
    def __init__(self) -> None:
        groups_entity = get_entity(EntityKey.GROUPS)
        super().__init__(default_entity=groups_entity)
