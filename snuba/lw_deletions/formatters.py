from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Mapping, MutableMapping, Sequence, Type

from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.bulk_delete_query import DeleteQueryMessage
from snuba.web.delete_query import ConditionsType


class Formatter(ABC):
    """
    Simple class with just a format method, which should
    be implemented for each storage type used for deletes.

    The `format` method takes a list of batched up messages
    and formats the conditions for the storage, if needed.
    """

    @abstractmethod
    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsType]:
        raise NotImplementedError


class SearchIssuesFormatter(Formatter):
    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsType]:
        """
        For the search issues storage we want the additional
        formatting step of combining group ids for messages
        that have the same project id.

        ex.
            project_id [1] and group_id [1, 2]
            project_id [1] and group_id [3, 4]

        would be grouped into one condition:

            project_id [1] and group_id [1, 2, 3, 4]

        """
        mapping: MutableMapping[int, set[int]] = defaultdict(set)
        conditions = [message["conditions"] for message in messages]
        for condition in conditions:
            project_id = condition["project_id"][0]
            # appease mypy
            assert isinstance(project_id, int)
            mapping[project_id] = mapping[project_id].union(
                # using int() to make mypy happy
                set([int(g_id) for g_id in condition["group_id"]])
            )

        return [
            {"project_id": [project_id], "group_id": list(group_ids)}
            for project_id, group_ids in mapping.items()
        ]


class EAPItemsFormatter(Formatter):
    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsType]:
        return [msg["conditions"] for msg in messages]


STORAGE_FORMATTER: Mapping[str, Type[Formatter]] = {
    StorageKey.SEARCH_ISSUES.value: SearchIssuesFormatter,
    StorageKey.EAP_ITEMS.value: EAPItemsFormatter,
}
