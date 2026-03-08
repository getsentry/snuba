from abc import ABC, abstractmethod
from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.types import AttributeConditions, ConditionsBag
from snuba.web.bulk_delete_query import DeleteQueryMessage, WireAttributeCondition


class Formatter(ABC):
    """
    Simple class with just a format method, which should
    be implemented for each storage type used for deletes.

    The `format` method takes a list of batched up messages
    and formats the conditions for the storage, if needed.
    """

    @abstractmethod
    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsBag]:
        raise NotImplementedError


class SearchIssuesFormatter(Formatter):
    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsBag]:
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
            ConditionsBag(
                column_conditions={
                    "project_id": [project_id],
                    "group_id": list(group_ids),
                }
            )
            for project_id, group_ids in mapping.items()
        ]


def _deserialize_attribute_conditions(
    data: Optional[Dict[str, WireAttributeCondition]],
    item_type: Optional[int] = None,
) -> Optional[AttributeConditions]:
    if data is None:
        return None
    assert item_type is not None, "attribute_conditions cannot be deserialized without item_type"

    attributes: Dict[str, Tuple[AttributeKey, List[Any]]] = {}

    for key, wire_condition in data.items():
        attr_key_type = wire_condition["attr_key_type"]
        attr_key_name = wire_condition["attr_key_name"]
        attr_values = list(wire_condition["attr_values"])
        attr_key_enum = AttributeKey(
            type=AttributeKey.Type.ValueType(attr_key_type), name=attr_key_name
        )
        attributes[key] = (attr_key_enum, attr_values)

    return AttributeConditions(
        item_type=item_type,
        attributes=attributes,
    )


class EAPItemsFormatter(Formatter):
    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsBag]:
        return [
            ConditionsBag(
                column_conditions=msg["conditions"],
                attribute_conditions=_deserialize_attribute_conditions(
                    msg.get("attribute_conditions"),
                    msg.get("attribute_conditions_item_type"),
                ),
            )
            for msg in messages
        ]


STORAGE_FORMATTER: Mapping[str, Type[Formatter]] = {
    StorageKey.SEARCH_ISSUES.value: SearchIssuesFormatter,
    StorageKey.EAP_ITEMS.value: EAPItemsFormatter,
}
