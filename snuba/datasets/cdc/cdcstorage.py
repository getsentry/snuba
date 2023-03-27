from typing import Any

from snuba.datasets.cdc.row_processors import CdcRowProcessor
from snuba.datasets.storage import WritableTableStorage


class CdcStorage(WritableTableStorage):
    def __init__(
        self,
        *,
        default_control_topic: str,
        postgres_table: str,
        row_processor: CdcRowProcessor,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.__default_control_topic = default_control_topic
        self.__postgres_table = postgres_table
        self.__row_processor = row_processor

    def get_row_processor(self) -> CdcRowProcessor:
        return self.__row_processor

    def get_postgres_table(self) -> str:
        return self.__postgres_table
