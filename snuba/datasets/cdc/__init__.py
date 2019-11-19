from snuba.datasets.dataset import Dataset
from snuba.datasets.dataset_schemas import DatasetSchemas


class CdcDataset(Dataset):
    def __init__(
        self,
        dataset_schemas: DatasetSchemas,
        *,
        default_control_topic: str,
        postgres_table: str,
        **kwargs
    ):
        super().__init__(
            dataset_schemas=dataset_schemas, **kwargs,
        )
        self.__default_control_topic = default_control_topic
        self.__postgres_table = postgres_table

    def get_default_control_topic(self) -> str:
        return self.__default_control_topic

    def get_postgres_table(self) -> str:
        return self.__postgres_table
