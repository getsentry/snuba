from typing import Mapping, Any

from snuba import settings, util
from snuba.state import get_config
from snuba.replacer import get_projects_query_flags
from snuba.query.extensions import ExtensionQueryProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.query_processor import ExtensionData
from snuba.query.query import Query


PROJECT_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        'project': {
            'anyOf': [
                {'type': 'integer', 'minimum': 1},
                {
                    'type': 'array',
                    'items': {'type': 'integer', 'minimum': 1},
                    'minItems': 1,
                },
            ]
        },
    },
    # Need to select down to the project level for customer isolation and performance
    'required': ['project'],
    'additionalProperties': False,
}


class ProjectExtensionProcessor(ExtensionQueryProcessor):

    def process_query(
            self,
            query: Query,
            extension_data: ExtensionData,
            request_settings: Mapping[str, bool],
            query_hints: Mapping[str, Any],
            stats: Mapping[str, Any],
    ) -> None:
        # NOTE: we rely entirely on the schema to make sure that regular snuba
        # queries are required to send a project_id filter. Some other special
        # internal query types do not require a project_id filter.
        project_ids = util.to_list(extension_data['project'])
        stats.update({'num_projects': len(project_ids)})

        if project_ids:
            query.add_conditions([('project_id', 'IN', project_ids)])

        turbo = request_settings.get('turbo', False)
        if not turbo:
            final, exclude_group_ids = get_projects_query_flags(project_ids)
            if not final and exclude_group_ids:
                # If the number of groups to exclude exceeds our limit, the query
                # should just use final instead of the exclusion set.
                max_group_ids_exclude = get_config('max_group_ids_exclude', settings.REPLACER_MAX_GROUP_IDS_TO_EXCLUDE)
                if len(exclude_group_ids) > max_group_ids_exclude:
                    query_hints.update({'final': True})
                else:
                    query_hints.update({'final': False})
                    query.add_conditions([(['assumeNotNull', ['group_id']], 'NOT IN', exclude_group_ids)])
            else:
                query_hints.update({'final': final})


class ProjectExtension(QueryExtension):
    def __init__(self) -> None:
        super().__init__(
            schema=PROJECT_EXTENSION_SCHEMA,
            processor=ProjectExtensionProcessor(),
        )
