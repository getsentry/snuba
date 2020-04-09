from snuba.datasets.storage import WritableTableStorage


class CdcStorage(WritableTableStorage):
    def __init__(
        self, *, default_control_topic: str, postgres_table: str, **kwargs,
    ):
        super().__init__(**kwargs)
        self.__default_control_topic = default_control_topic
        self.__postgres_table = postgres_table

    def get_default_control_topic(self) -> str:
        return self.__default_control_topic

    def get_postgres_table(self) -> str:
        return self.__postgres_table
