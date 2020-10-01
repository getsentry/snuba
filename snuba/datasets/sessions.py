from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey, get_entity


class SessionsDataset(Dataset):
    def __init__(self) -> None:
        sessions_entity = get_entity(EntityKey.SESSIONS)
        super().__init__(default_entity=sessions_entity)
