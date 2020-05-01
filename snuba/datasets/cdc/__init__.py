from snuba.datasets.storage import WritableTableStorage
from snuba.snapshots.loaders.single_table import RowProcessor


class CdcStorage(WritableTableStorage):
    def __init__(
        self,
        *,
        default_control_topic: str,
        postgres_table: str,
        row_processor: RowProcessor,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.__default_control_topic = default_control_topic
        self.__postgres_table = postgres_table
        self.__row_processor = row_processor

    def get_row_processor(self) -> RowProcessor:
        return self.__row_processor

    def get_default_control_topic(self) -> str:
        return self.__default_control_topic

    def get_postgres_table(self) -> str:
        return self.__postgres_table
