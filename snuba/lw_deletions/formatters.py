from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Mapping, MutableMapping, Sequence, Type

from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.hashes import fnv_1a
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
    # Number of attribute buckets used in eap_items storage
    # TODO: find a way to wire this in from a canonical source
    NUM_ATTRIBUTE_BUCKETS = 40

    def format(self, messages: Sequence[DeleteQueryMessage]) -> Sequence[ConditionsType]:
        """
        For eap_items storage, we need to resolve attribute_conditions to their
        bucketed column names. Attributes are stored in hash-bucketed map columns
        like attributes_string_0, attributes_string_1, etc.

        For example, if attribute_conditions has {"group_id": [123]}, we need to:
        1. Determine which bucket "group_id" belongs to
        2. Add it to the conditions as attributes_string_{bucket_idx}['group_id'] = [123]
        """
        formatted_conditions: List[ConditionsType] = []

        for message in messages:
            conditions = dict(message["conditions"])

            # Process attribute_conditions if present
            if "attribute_conditions" in message and message["attribute_conditions"]:
                attribute_conditions = message["attribute_conditions"]

                # For each attribute, determine its bucket and add to conditions
                for attr_name, attr_values in attribute_conditions.items():
                    # Hash the attribute name to determine which bucket it belongs to
                    bucket_idx = fnv_1a(attr_name.encode("utf-8")) % self.NUM_ATTRIBUTE_BUCKETS

                    # Create the bucketed column name with the attribute key
                    # Format: "attributes_string_{bucket_idx}['{attr_name}']"
                    bucketed_column = f"attributes_string_{bucket_idx}['{attr_name}']"

                    conditions[bucketed_column] = attr_values

            formatted_conditions.append(conditions)

        return formatted_conditions


STORAGE_FORMATTER: Mapping[str, Type[Formatter]] = {
    StorageKey.SEARCH_ISSUES.value: SearchIssuesFormatter,
    # TODO: We will probably do something more sophisticated here in the future
    # but it won't make much of a difference until we support delete by attribute
    StorageKey.EAP_ITEMS.value: EAPItemsFormatter,
}
