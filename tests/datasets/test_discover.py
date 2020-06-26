import pytest
from typing import Any, MutableMapping
from tests.base import BaseDatasetTest

from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
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
    ({"conditions": [["type", "=", "transaction"]]}, "transactions_local",),
    ({"conditions": [["type", "!=", "transaction"]]}, "sentry_local",),
    ({"conditions": []}, "sentry_local",),
    ({"conditions": [["duration", "=", 0]]}, "transactions_local",),
    (
        {"conditions": [["event_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        "transactions_local",
    ),
    # No conditions, other referenced columns
    ({"selected_columns": ["group_id"]}, "sentry_local"),
    ({"selected_columns": ["trace_id"]}, "transactions_local"),
    ({"selected_columns": ["group_id", "trace_id"]}, "sentry_local"),
    ({"aggregations": [["max", "duration", "max_duration"]]}, "transactions_local"),
    (
        {"aggregations": [["apdex(duration, 300)", None, "apdex_duration_300"]]},
        "transactions_local",
    ),
]


class TestDiscover(BaseDatasetTest):
    @pytest.mark.parametrize("query_body, expected_table", test_data)
    def test_data_source(
        self, query_body: MutableMapping[str, Any], expected_table: str,
    ):
        request_settings = HTTPRequestSettings()
        dataset = get_dataset("discover")
        query = parse_query(query_body, dataset)
        request = Request("a", query, request_settings, {}, "r")
        for processor in get_dataset("discover").get_query_processors():
            processor.process_query(request.query, request.settings)

        plan = dataset.get_query_plan_builder().build_plan(request)

        for physical_processor in plan.plan_processors:
            physical_processor.process_query(plan.query, request.settings)

        assert plan.query.get_data_source().format_from() == expected_table
