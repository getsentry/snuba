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
        appropriate column names based on type:
        - int/bool: Single columns (no bucketing) like attributes_int['key'] or attributes_bool['key']
        - string/float: Hash-bucketed columns like attributes_string_0['key'] or attributes_float_23['key']

        For example, if attribute_conditions has {"group_id": [123]}, we need to:
        1. Determine the type based on the values (int in this case)
        2. Since int doesn't use bucketing, add it as attributes_int['group_id'] = [123]
        """
        formatted_conditions: List[ConditionsType] = []

        for message in messages:
            conditions = dict(message["conditions"])

            # Process attribute_conditions if present
            if "attribute_conditions" in message and message["attribute_conditions"]:
                attribute_conditions = message["attribute_conditions"]

                # For each attribute, determine its type and bucket (if applicable)
                for attr_name, attr_values in attribute_conditions.items():
                    if not attr_values:
                        continue

                    # Determine the attribute type from the first value
                    # All values in the list should be of the same type
                    first_value = attr_values[0]
                    if isinstance(first_value, bool):
                        # Check bool before int since bool is a subclass of int in Python
                        attr_type = "bool"
                    elif isinstance(first_value, int):
                        attr_type = "int"
                    elif isinstance(first_value, float):
                        attr_type = "float"
                    else:
                        # Default to string for str and any other type
                        attr_type = "string"

                    # Only string and float attributes use bucketing
                    # int and bool attributes are stored in single columns
                    if attr_type in ("int", "bool"):
                        # No bucketing for int and bool
                        bucketed_column = f"attributes_{attr_type}['{attr_name}']"
                    else:
                        # Bucketing for string and float
                        bucket_idx = fnv_1a(attr_name.encode("utf-8")) % self.NUM_ATTRIBUTE_BUCKETS
                        bucketed_column = f"attributes_{attr_type}_{bucket_idx}['{attr_name}']"

                    conditions[bucketed_column] = attr_values

            formatted_conditions.append(conditions)

        return formatted_conditions


STORAGE_FORMATTER: Mapping[str, Type[Formatter]] = {
    StorageKey.SEARCH_ISSUES.value: SearchIssuesFormatter,
    # TODO: We will probably do something more sophisticated here in the future
    # but it won't make much of a difference until we support delete by attribute
    StorageKey.EAP_ITEMS.value: EAPItemsFormatter,
}
