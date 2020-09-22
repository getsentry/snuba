from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity


class TransactionsDataset(Dataset):
    def __init__(self) -> None:
        transactions_entity = get_entity(EntityKey.TRANSACTIONS)
        super().__init__(default_entity=transactions_entity)
