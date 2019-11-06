import pytest
from typing import Sequence
from tests.base import BaseDatasetTest

from snuba.datasets.factory import get_dataset
from snuba.query.query import Condition, Query
from snuba.request.request_settings import RequestSettings


def get_dataset_source(dataset_name):
    return get_dataset(dataset_name) \
        .get_dataset_schemas() \
        .get_read_schema() \
        .get_data_source()


test_data = [
    (
        [["type", "=", "transaction"]],
        "transactions",
    ),
    (
        [["type", "!=", "transaction"]],
        "events",
    ),
    (
        [],
        "events",
    ),
    (
        [["duration", "=", 0]],
        "transactions",
    ),
]


class TestDiscover(BaseDatasetTest):
    @pytest.mark.parametrize("conditions, expected_dataset", test_data)
    def test_data_source(self, conditions: Sequence[Condition], expected_dataset: str):
        query = Query(
            {
                "conditions": conditions,
            },
            get_dataset_source("discover")
        )

        request_settings = RequestSettings(turbo=False, consistent=False, debug=False)
        for processor in get_dataset("discover").get_query_processors():
            processor.process_query(query, request_settings)

        assert query.get_data_source().format_from() == get_dataset_source(expected_dataset).format_from()
