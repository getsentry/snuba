from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity


class ErrorsDataset(Dataset):
    def __init__(self) -> None:
        errors_entity = get_entity(EntityKey.ERRORS)
        super().__init__(entities=[errors_entity])
