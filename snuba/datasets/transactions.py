from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey


class TransactionsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.TRANSACTIONS)
