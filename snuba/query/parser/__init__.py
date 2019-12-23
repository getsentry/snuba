from typing import Any, MutableMapping

from snuba.datasets.dataset import Dataset
from snuba.query.query import Query


def parse_query(body: MutableMapping[str, Any], dataset: Dataset,) -> Query:
    source = dataset.get_dataset_schemas().get_read_schema().get_data_source()
    return Query(body, source)
