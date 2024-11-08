from typing import Sequence, Type

import pytest

from snuba.lw_deletions.formatters import Formatter, SearchIssuesFormatter
from snuba.web.bulk_delete_query import DeleteQueryMessage
from snuba.web.delete_query import ConditionsType


def create_delete_query_message(conditions: ConditionsType) -> DeleteQueryMessage:
    return DeleteQueryMessage(
        rows_to_delete=1,
        tenant_ids={},
        conditions=conditions,
        storage_name="search_issues",
    )


SEARCH_ISSUES_FORMATTER = SearchIssuesFormatter


@pytest.mark.parametrize(
    "messages, expected_formatted, formatter",
    [
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "group_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [1], "group_id": [4, 5, 6]}),
            ],
            [
                {"project_id": [1], "group_id": [1, 2, 3, 4, 5, 6]},
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_combine_group_ids_same_project",
        ),
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "group_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [2], "group_id": [3]}),
            ],
            [
                {"project_id": [1], "group_id": [1, 2, 3]},
                {"project_id": [2], "group_id": [3]},
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_diff_projects_dont_combine",
        ),
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "group_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [1], "group_id": [2, 3, 4]}),
            ],
            [
                {"project_id": [1], "group_id": [1, 2, 3, 4]},
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_dedupe_group_ids_in_same_project",
        ),
    ],
)
def test_search_issues_formatter(
    messages: Sequence[DeleteQueryMessage],
    expected_formatted: Sequence[ConditionsType],
    formatter: Type[Formatter],
) -> None:
    formatted = formatter().format(messages)
    assert formatted == expected_formatted
