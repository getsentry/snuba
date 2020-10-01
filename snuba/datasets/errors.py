from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey, get_entity


class ErrorsDataset(Dataset):
    def __init__(self) -> None:
        errors_entity = get_entity(EntityKey.ERRORS)
        super().__init__(default_entity=errors_entity)
