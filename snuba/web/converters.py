from werkzeug.routing import BaseConverter

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset, get_dataset_name


class DatasetConverter(BaseConverter):
    def to_python(self, value: str):
        return get_dataset(value)

    def to_url(self, value: Dataset):
        return get_dataset_name(value)
