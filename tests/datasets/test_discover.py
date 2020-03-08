import pytest
from typing import Any, MutableMapping
from tests.base import BaseDatasetTest

from snuba.datasets.factory import get_dataset
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings


def get_dataset_source(dataset_name):
    return (
        get_dataset(dataset_name)
        .get_dataset_schemas()
        .get_read_schema()
        .get_data_source()
    )


test_data = [
    ({"conditions": [["type", "=", "transaction"]]}, "transactions",),
    ({"conditions": [["type", "!=", "transaction"]]}, "events",),
    ({"conditions": []}, "events",),
    ({"conditions": [["duration", "=", 0]]}, "transactions",),
    # No conditions, other referenced columns
    ({"selected_columns": ["group_id"]}, "events"),
    ({"selected_columns": ["trace_id"]}, "transactions"),
    ({"selected_columns": ["group_id", "trace_id"]}, "events"),
    ({"aggregations": [["max", "duration", "max_duration"]]}, "transactions"),
]


class TestDiscover(BaseDatasetTest):
    @pytest.mark.parametrize("query_body, expected_dataset", test_data)
    def test_data_source(
        self, query_body: MutableMapping[str, Any], expected_dataset: str
    ):
        query = Query(query_body, None)
        request_settings = HTTPRequestSettings()
        dataset = get_dataset_source("discover")
        storage = dataset.get_query_storage_selector().select_storage(
            query, request_settings
        )
        query.set_source(storage.get_source())

        for processor in get_dataset("discover").get_query_processors():
            processor.process_query(query, request_settings)

        assert query.get_data_source().format_from() == expected_dataset
