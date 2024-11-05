from abc import ABC, abstractmethod
from typing import Mapping, MutableMapping, Sequence, Type

from attr import dataclass

from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.bulk_delete_query import ConditionsType, DeleteQueryMessage


class Formatter(ABC):
    @abstractmethod
    def format(
        self, messages: Sequence[DeleteQueryMessage]
    ) -> Sequence[ConditionsType]:
        raise NotImplementedError


@dataclass
class SearchIssueCondition:
    project_id: int
    group_ids: list[int] = []

    def add_group_ids(self, group_ids: Sequence[int]) -> None:
        for group_id in group_ids:
            self.group_ids.append(group_id)


class SearchIssuesFormatter(Formatter):
    def format(
        self, messages: Sequence[DeleteQueryMessage]
    ) -> Sequence[ConditionsType]:
        mapping: MutableMapping[int, SearchIssueCondition] = {}
        for message in messages:
            project_id = message["conditions"]["project_id"][0]
            group_ids = message["conditions"]["group_id"]
            # make mypy happy
            assert isinstance(project_id, int)
            assert isinstance(group_ids, list)

            and_condition = mapping.get(
                project_id, SearchIssueCondition(project_id, [])
            )
            and_condition.add_group_ids(group_ids)
            mapping[project_id] = and_condition

        and_conditions: list[ConditionsType] = []
        for and_condition in mapping.values():
            and_conditions.append(
                {
                    "project_id": [and_condition.project_id],
                    "group_id": and_condition.group_ids,
                }
            )
        return and_conditions


STORAGE_FORMATTER: Mapping[str, Type[Formatter]] = {
    StorageKey.SEARCH_ISSUES.value: SearchIssuesFormatter
}
