from base import BaseTest

import pytest
from typing import Sequence

from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsExtension
from snuba.query.query import Query, Condition, QueryHints
from snuba.schemas import validate_jsonschema
from snuba import replacer, state
from snuba.redis import redis_client


project_extension_test_data = [
    (
        {
            'project': 2
        },
        [
            ('project_id', 'IN', [2])
        ]
    ),
    (
        {
            'project': [2, 3]
        },
        [
            ('project_id', 'IN', [2, 3])
        ]
    )
]


@pytest.mark.parametrize("raw_data, expected_conditions", project_extension_test_data)
def test_project_extension_query_processing(raw_data: dict, expected_conditions: Sequence[Condition]):
    extension = ProjectExtension()
    valid_data = validate_jsonschema(raw_data, extension.get_schema())
    query = Query({
        "conditions": []
    })
    query_hints = QueryHints(turbo=False, final=False)

    extension.get_processor().process_query(query, valid_data, query_hints)
    assert query.get_conditions() == expected_conditions


class TestProjectExtensionWithGroups(BaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        raw_data = {'project': 2}
        
        self.extension = ProjectWithGroupsExtension()
        self.valid_data = validate_jsonschema(raw_data, self.extension.get_schema())
        self.query = Query({
            "conditions": []
        })

    def test_with_turbo(self):
        query_hints = QueryHints(turbo=True, final=False)

        self.extension.get_processor().process_query(self.query, self.valid_data, query_hints)
        assert self.query.get_conditions() == [('project_id', 'IN', [2])]

    def test_without_turbo_with_projects_needing_final(self):
        query_hints = QueryHints(turbo=False, final=False)
        replacer.set_project_needs_final(2)

        self.extension.get_processor().process_query(self.query, self.valid_data, query_hints)
        assert self.query.get_conditions() == [('project_id', 'IN', [2])]
        assert query_hints.final

    def test_without_turbo_without_projects_needing_final(self):
        query_hints = QueryHints(turbo=False, final=False)

        self.extension.get_processor().process_query(self.query, self.valid_data, query_hints)
        assert self.query.get_conditions() == [('project_id', 'IN', [2])]
        assert not query_hints.final

    def test_when_there_are_not_many_groups_to_exclude(self):
        query_hints = QueryHints(turbo=False, final=False)
        state.set_config('max_group_ids_exclude', 5)
        replacer.set_project_exclude_groups(2, [100, 101, 102])

        self.extension.get_processor().process_query(self.query, self.valid_data, query_hints)

        expected = [
            ('project_id', 'IN', [2]),
            (['assumeNotNull', ['group_id']], 'NOT IN', [100, 101, 102])
        ]

        assert self.query.get_conditions() == expected
        assert not query_hints.final

    def test_when_there_are_too_many_groups_to_exclude(self):
        query_hints = QueryHints(turbo=False, final=False)
        state.set_config('max_group_ids_exclude', 2)
        replacer.set_project_exclude_groups(2, [100, 101, 102])

        self.extension.get_processor().process_query(self.query, self.valid_data, query_hints)

        assert self.query.get_conditions() == [('project_id', 'IN', [2])]
        assert query_hints.final
