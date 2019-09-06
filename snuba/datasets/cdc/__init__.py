from snuba.datasets import Dataset
from snuba.datasets.schema import TableSchema
from snuba.datasets.cdc.cdcprocessors import CdcProcessor


class CdcDataset(Dataset):
    def __init__(self,
            schema: TableSchema, *,
            processor: CdcProcessor,
            default_control_topic: str,
            **kwargs):
        super().__init__(
            schema=schema,
            processor=processor,
            **kwargs,
        )
        self.__default_control_topic = default_control_topic

    def get_default_control_topic(self):
        return self.__default_control_topic
