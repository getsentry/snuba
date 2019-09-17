from snuba.datasets import Dataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.cdc.cdcprocessors import CdcProcessor


class CdcDataset(Dataset):
    def __init__(self,
            dataset_schemas: DatasetSchemas, *,
            processor: CdcProcessor,
            default_control_topic: str,
            **kwargs):
        super().__init__(
            dataset_schemas=dataset_schemas,
            processor=processor,
            **kwargs,
        )
        self.__default_control_topic = default_control_topic

    def get_default_control_topic(self):
        return self.__default_control_topic
