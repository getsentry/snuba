import pytest
from typing import Sequence

from base import BaseTest
from snuba import replacer, state
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor, ProjectWithGroupsProcessor
from snuba.query.query import Query, Condition
from snuba.request.request_query_settings import RequestQuerySettings
from snuba.schemas import validate_jsonschema

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
    extension = ProjectExtension(
        processor=ProjectExtensionProcessor()
    )
    valid_data = validate_jsonschema(raw_data, extension.get_schema())
    query = Query({
        "conditions": []
    })
    request_query_settings = RequestQuerySettings(turbo=False, consistent=False, debug=False)

    extension.get_processor().process_query(query, valid_data, request_query_settings)

    assert query.get_conditions() == expected_conditions


class TestProjectExtensionWithGroups(BaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        raw_data = {'project': 2}
        
        self.extension = ProjectExtension(
            processor=ProjectWithGroupsProcessor()
        )
        self.valid_data = validate_jsonschema(raw_data, self.extension.get_schema())
        self.query = Query({
            "conditions": []
        })

    def test_with_turbo(self):
        request_query_settings = RequestQuerySettings(turbo=True, consistent=False, debug=False)

        self.extension.get_processor().process_query(self.query, self.valid_data, request_query_settings)

        assert self.query.get_conditions() == [('project_id', 'IN', [2])]

    def test_without_turbo_with_projects_needing_final(self):
        request_query_settings = RequestQuerySettings(turbo=False, consistent=False, debug=False)
        replacer.set_project_needs_final(2)

        self.extension.get_processor().process_query(self.query, self.valid_data, request_query_settings)

        assert self.query.get_conditions() == [('project_id', 'IN', [2])]
        assert self.query.get_final()

    def test_without_turbo_without_projects_needing_final(self):
        request_query_settings = RequestQuerySettings(turbo=False, consistent=False, debug=False)

        self.extension.get_processor().process_query(self.query, self.valid_data, request_query_settings)

        assert self.query.get_conditions() == [('project_id', 'IN', [2])]
        assert not self.query.get_final()

    def test_when_there_are_not_many_groups_to_exclude(self):
        request_query_settings = RequestQuerySettings(turbo=False, consistent=False, debug=False)
        state.set_config('max_group_ids_exclude', 5)
        replacer.set_project_exclude_groups(2, [100, 101, 102])

        self.extension.get_processor().process_query(self.query, self.valid_data, request_query_settings)

        expected = [
            ('project_id', 'IN', [2]),
            (['assumeNotNull', ['group_id']], 'NOT IN', [100, 101, 102])
        ]
        assert self.query.get_conditions() == expected
        assert not self.query.get_final()

    def test_when_there_are_too_many_groups_to_exclude(self):
        request_query_settings = RequestQuerySettings(turbo=False, consistent=False, debug=False)
        state.set_config('max_group_ids_exclude', 2)
        replacer.set_project_exclude_groups(2, [100, 101, 102])

        self.extension.get_processor().process_query(self.query, self.valid_data, request_query_settings)

        assert self.query.get_conditions() == [('project_id', 'IN', [2])]
        assert self.query.get_final()
