from snuba.datasets import Dataset
from snuba.datasets.dataset_schemas import DatasetSchemas


class CdcDataset(Dataset):
    def __init__(self,
            dataset_schemas: DatasetSchemas, *,
            default_control_topic: str,
            **kwargs):
        super().__init__(
            dataset_schemas=dataset_schemas,
            **kwargs,
        )
        self.__default_control_topic = default_control_topic

    def get_default_control_topic(self):
        return self.__default_control_topic
