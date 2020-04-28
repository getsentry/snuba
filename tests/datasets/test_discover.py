import pytest
from typing import Any, MutableMapping
from tests.base import BaseDatasetTest

from snuba.datasets.factory import get_dataset
from snuba.query.query import Query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings


def get_dataset_source(dataset_name):
    return (
        get_dataset(dataset_name)
        .get_all_storages()[0]
        .get_schemas()
        .get_read_schema()
        .get_data_source()
    )


test_data = [
    ({"conditions": [["type", "=", "transaction"]]}, "test_transactions_local",),
    ({"conditions": [["type", "!=", "transaction"]]}, "test_sentry_local",),
    ({"conditions": []}, "test_sentry_local",),
    ({"conditions": [["duration", "=", 0]]}, "test_transactions_local",),
    (
        {"conditions": [["event_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        "test_transactions_local",
    ),
    # No conditions, other referenced columns
    ({"selected_columns": ["group_id"]}, "test_sentry_local"),
    ({"selected_columns": ["trace_id"]}, "test_transactions_local"),
    ({"selected_columns": ["group_id", "trace_id"]}, "test_sentry_local"),
    (
        {"aggregations": [["max", "duration", "max_duration"]]},
        "test_transactions_local",
    ),
]


class TestDiscover(BaseDatasetTest):
    @pytest.mark.parametrize("query_body, expected_table", test_data)
    def test_data_source(
        self, query_body: MutableMapping[str, Any], expected_table: str
    ):
        query = Query(query_body, None)
        request_settings = HTTPRequestSettings()
        dataset = get_dataset("discover")
        request = Request("a", query, request_settings, {}, "r")
        for processor in get_dataset("discover").get_query_processors():
            processor.process_query(request.query, request.settings)

        plan = dataset.get_query_plan_builder().build_plan(request)

        for processor in [*plan.plan_processors]:
            processor.process_query(request.query, request.settings)

        assert request.query.get_data_source().format_from() == expected_table
