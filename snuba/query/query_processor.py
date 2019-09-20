from abc import ABC, abstractmethod
from typing import Any, Generic, Mapping, TypeVar

from snuba.query.query import Query
from snuba import state, settings, util
from snuba.replacer import get_projects_query_flags

ExtensionData = Mapping[str, Any]

TQueryProcessContext = TypeVar("TQueryProcessContext")


class QueryProcessor(ABC, Generic[TQueryProcessContext]):
    """
    Base class for all query processors. Whatever extends this
    class is supposed to provide one method that takes a query
    object of type Query that represent the parsed query to
    process, a context (which depends on the processor) and
    updates it.
    """

    @abstractmethod
    def process_query(self,
        query: Query,
        context_data: TQueryProcessContext,
        request_settings: Mapping[str, bool]
    ) -> None:
        # TODO: Now the query is moved around through the Request object, which
        # is frozen (and it should be), thus the Query itself is mutable since
        # we cannot reassign it.
        # Ideally this should return a query insteadof assuming it mutates the
        # existing one in place. We can move towards an immutable structure
        # after changing Request.
        raise NotImplementedError


class ExtensionQueryProcessor(QueryProcessor[ExtensionData]):
    """
    Common parent class for all the extension processors. The only
    contribution of this class is to resolve the generic context to
    extension data. So subclasses of this one can be used to process
    query extensions.
    """

    @abstractmethod
    def process_query(self, query: Query, extension_data: ExtensionData, request_settings: Mapping[str, bool]) -> None:
        raise NotImplementedError


class DummyExtensionProcessor(ExtensionQueryProcessor):

    def process_query(self, query: Query, extension_data: ExtensionData, request_settings: Mapping[str, bool]) -> None:
        return query


class ProjectExtensionProcessor(ExtensionQueryProcessor):

    # TODO(manu): make sure this is fine by https://github.com/getsentry/snuba/pull/473
    def process_query(self, query: Query, extension_data: ExtensionData, request_settings: Mapping[str, bool]) -> None:
        # NOTE: we rely entirely on the schema to make sure that regular snuba
        # queries are required to send a project_id filter. Some other special
        # internal query types do not require a project_id filter.
        project_ids = util.to_list(extension_data['project'])
        if project_ids:
            query.add_conditions([('project_id', 'IN', project_ids)])

        turbo = request_settings.get('turbo', False)
        if not turbo:
            final, exclude_group_ids = get_projects_query_flags(project_ids)
            if not final and exclude_group_ids:
                # If the number of groups to exclude exceeds our limit, the query
                # should just use final instead of the exclusion set.
                max_group_ids_exclude = state.get_config('max_group_ids_exclude',
                                                         settings.REPLACER_MAX_GROUP_IDS_TO_EXCLUDE)
                if len(exclude_group_ids) > max_group_ids_exclude:
                    query.set_final(True)
                else:
                    query.set_final(False)
                    query.add_conditions([(['assumeNotNull', ['group_id']], 'NOT IN', exclude_group_ids)])
            else:
                query.set_final(final)
