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
        query = Query(query_body, get_dataset_source("discover"))

        request_settings = HTTPRequestSettings()
        for processor in get_dataset("discover").get_query_processors():
            processor.process_query(query, request_settings)

        assert (
            query.get_data_source().format_from()
            == get_dataset_source(expected_dataset).format_from()
        )
