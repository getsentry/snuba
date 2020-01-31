import pytest
from typing import Any, MutableMapping
from tests.base import BaseDatasetTest

from snuba import settings, state
from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
from snuba.request.request_settings import HTTPRequestSettings


def get_dataset_source(dataset_name):
    return (
        get_dataset(dataset_name)
        .get_dataset_schemas()
        .get_read_schema()
        .get_data_source()
    )


test_data = [
    (
        {"conditions": [["project_id", "IN", [1]], ["type", "=", "transaction"]]},
        "transactions",
    ),
    (
        {"conditions": [["project_id", "IN", [1]], ["type", "!=", "transaction"]]},
        "events",
    ),
    ({"conditions": [["project_id", "IN", [1]]]}, "events",),
    (
        {"conditions": [["project_id", "IN", [1]], ["duration", "=", 0]]},
        "transactions",
    ),
    # No conditions, other referenced columns
    (
        {"selected_columns": ["group_id"], "conditions": [["project_id", "IN", [1]]]},
        "events",
    ),
    (
        {"selected_columns": ["trace_id"], "conditions": [["project_id", "IN", [1]]]},
        "transactions",
    ),
    (
        {
            "selected_columns": ["group_id", "trace_id"],
            "conditions": [["project_id", "IN", [1]]],
        },
        "events",
    ),
    (
        {
            "conditions": [["project_id", "IN", [1]]],
            "aggregations": [["max", "duration", "max_duration"]],
        },
        "transactions",
    ),
]


class TestDiscover(BaseDatasetTest):
    @pytest.mark.parametrize("query_body, expected_dataset", test_data)
    def test_data_source(
        self, query_body: MutableMapping[str, Any], expected_dataset: str
    ):
        settings.MAX_PREWHERE_CONDITIONS = 3
        state.set_config("prewhere_custom_key_projects", "[1,3]")

        dataset = get_dataset("discover")
        query = parse_query(query_body, dataset)
        request_settings = HTTPRequestSettings()

        for processor in get_dataset("discover").get_query_processors():
            processor.process_query(query, request_settings)

        assert (
            query.get_data_source().format_from()
            == get_dataset_source(expected_dataset).format_from()
        )
