from snuba.datasets.dataset import Dataset
from snuba.datasets.schemas import ColumnSet
from snuba.datasets.storage import QueryStorageSelector


class CdcDataset(Dataset):
    def __init__(
        self,
        *,
        storage_selector: QueryStorageSelector,
        abstract_column_set: ColumnSet,
        default_control_topic: str,
        postgres_table: str,
        **kwargs
    ):
        super().__init__(
            storage_selector=storage_selector,
            abstract_column_set=abstract_column_set,
            **kwargs,
        )
        self.__default_control_topic = default_control_topic
        self.__postgres_table = postgres_table

    def get_default_control_topic(self) -> str:
        return self.__default_control_topic

    def get_postgres_table(self) -> str:
        return self.__postgres_table
