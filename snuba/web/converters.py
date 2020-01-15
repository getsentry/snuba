from werkzeug.routing import BaseConverter

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset


class DatasetConverter(BaseConverter):
    def to_python(self, value: str):
        return get_dataset(value)

    def to_url(self, value: Dataset):
        # This isn't needed right now, but should be straightforward to
        # implement if necessary once there is a way to map a dataset instance
        # back to it's name. (Basically, the inverse of ``get_dataset``.)
        raise NotImplementedError
