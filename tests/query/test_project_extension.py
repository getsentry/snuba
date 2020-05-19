import pytest
from typing import Sequence

from tests.base import BaseTest
from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.errors_replacer import (
    set_project_exclude_groups,
    set_project_needs_final,
    ReplacerState,
)
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import FunctionCall, BooleanFunctions
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.project_extension import (
    ProjectExtension,
    ProjectExtensionProcessor,
    ProjectWithGroupsProcessor,
)
from snuba.query.logical import Query
from snuba.query.types import Condition
from snuba.request.request_settings import HTTPRequestSettings
from snuba.schemas import validate_jsonschema


def build_in(project_column: str, projects: Sequence[int]) -> Expression:
    return FunctionCall(
        None,
        "in",
        (
            Column(None, None, project_column),
            FunctionCall(None, "tuple", tuple([Literal(None, p) for p in projects])),
        ),
    )


project_extension_test_data = [
    ({"project": 2}, [("project_id", "IN", [2])], build_in("project_id", [2]),),
    (
        {"project": [2, 3]},
        [("project_id", "IN", [2, 3])],
        build_in("project_id", [2, 3]),
    ),
]


@pytest.mark.parametrize(
    "raw_data, expected_conditions, expected_ast_conditions",
    project_extension_test_data,
)
def test_project_extension_query_processing(
    raw_data: dict,
    expected_conditions: Sequence[Condition],
    expected_ast_conditions: Expression,
):
    extension = ProjectExtension(
        processor=ProjectExtensionProcessor(project_column="project_id")
    )
    valid_data = validate_jsonschema(raw_data, extension.get_schema())
    query = Query({"conditions": []}, TableSource("my_table", ColumnSet([])),)
    request_settings = HTTPRequestSettings()

    extension.get_processor().process_query(query, valid_data, request_settings)

    assert query.get_conditions() == expected_conditions
    assert query.get_condition_from_ast() == expected_ast_conditions


def test_project_extension_query_adds_rate_limits():
    extension = ProjectExtension(
        processor=ProjectExtensionProcessor(project_column="project_id")
    )
    raw_data = {"project": [2, 3]}
    valid_data = validate_jsonschema(raw_data, extension.get_schema())
    query = Query({"conditions": []}, TableSource("my_table", ColumnSet([])),)
    request_settings = HTTPRequestSettings()

    num_rate_limits_before_processing = len(request_settings.get_rate_limit_params())
    extension.get_processor().process_query(query, valid_data, request_settings)

    rate_limits = request_settings.get_rate_limit_params()
    # make sure a rate limit was added by the processing
    assert len(rate_limits) == num_rate_limits_before_processing + 1

    most_recent_rate_limit = rate_limits[-1]
    assert most_recent_rate_limit.bucket == "2"
    assert most_recent_rate_limit.per_second_limit == 1000
    assert most_recent_rate_limit.concurrent_limit == 1000


def test_project_extension_project_rate_limits_are_overridden():
    extension = ProjectExtension(
        processor=ProjectExtensionProcessor(project_column="project_id")
    )
    raw_data = {"project": [2, 3]}
    valid_data = validate_jsonschema(raw_data, extension.get_schema())
    query = Query({"conditions": []}, TableSource("my_table", ColumnSet([])),)
    request_settings = HTTPRequestSettings()
    state.set_config("project_per_second_limit_2", 5)
    state.set_config("project_concurrent_limit_2", 10)

    extension.get_processor().process_query(query, valid_data, request_settings)

    rate_limits = request_settings.get_rate_limit_params()
    most_recent_rate_limit = rate_limits[-1]

    assert most_recent_rate_limit.bucket == "2"
    assert most_recent_rate_limit.per_second_limit == 5
    assert most_recent_rate_limit.concurrent_limit == 10


class TestProjectExtensionWithGroups(BaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        raw_data = {"project": 2}

        self.extension = ProjectExtension(
            processor=ProjectWithGroupsProcessor(
                project_column="project_id", replacer_state_name=ReplacerState.EVENTS
            )
        )
        self.valid_data = validate_jsonschema(raw_data, self.extension.get_schema())
        self.query = Query({"conditions": []}, TableSource("my_table", ColumnSet([])),)

    def test_with_turbo(self):
        request_settings = HTTPRequestSettings(turbo=True)

        self.extension.get_processor().process_query(
            self.query, self.valid_data, request_settings
        )

        assert self.query.get_conditions() == [("project_id", "IN", [2])]
        assert self.query.get_condition_from_ast() == build_in("project_id", [2])

    def test_without_turbo_with_projects_needing_final(self):
        request_settings = HTTPRequestSettings()
        set_project_needs_final(2, ReplacerState.EVENTS)

        self.extension.get_processor().process_query(
            self.query, self.valid_data, request_settings
        )

        assert self.query.get_conditions() == [("project_id", "IN", [2])]
        assert self.query.get_condition_from_ast() == build_in("project_id", [2])
        assert self.query.get_final()

    def test_without_turbo_without_projects_needing_final(self):
        request_settings = HTTPRequestSettings()

        self.extension.get_processor().process_query(
            self.query, self.valid_data, request_settings
        )

        assert self.query.get_conditions() == [("project_id", "IN", [2])]
        assert self.query.get_condition_from_ast() == build_in("project_id", [2])
        assert not self.query.get_final()

    def test_when_there_are_not_many_groups_to_exclude(self):
        request_settings = HTTPRequestSettings()
        state.set_config("max_group_ids_exclude", 5)
        set_project_exclude_groups(2, [100, 101, 102], ReplacerState.EVENTS)

        self.extension.get_processor().process_query(
            self.query, self.valid_data, request_settings
        )

        expected = [
            ("project_id", "IN", [2]),
            (["assumeNotNull", ["group_id"]], "NOT IN", [100, 101, 102]),
        ]
        assert self.query.get_conditions() == expected
        assert self.query.get_condition_from_ast() == FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    "notIn",
                    (
                        FunctionCall(
                            None, "assumeNotNull", (Column(None, None, "group_id"),)
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (
                                Literal(None, 100),
                                Literal(None, 101),
                                Literal(None, 102),
                            ),
                        ),
                    ),
                ),
                build_in("project_id", [2]),
            ),
        )
        assert not self.query.get_final()

    def test_when_there_are_too_many_groups_to_exclude(self):
        request_settings = HTTPRequestSettings()
        state.set_config("max_group_ids_exclude", 2)
        set_project_exclude_groups(2, [100, 101, 102], ReplacerState.EVENTS)

        self.extension.get_processor().process_query(
            self.query, self.valid_data, request_settings
        )

        assert self.query.get_conditions() == [("project_id", "IN", [2])]
        assert self.query.get_condition_from_ast() == build_in("project_id", [2])
        assert self.query.get_final()
