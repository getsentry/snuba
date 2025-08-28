from itertools import chain
from typing import Any, Sequence

from snuba.query.allocation_policies import AllocationPolicy, PolicyData


def convert(policy_data: PolicyData) -> dict[str, Any]:
    "We need to convert the policy data to the format that the frontend expects"
    return {
        "policy_name": policy_data["configurable_component_name"],
        "configs": policy_data["configurations"],
        "optional_config_definitions": policy_data["optional_config_definitions"],
        "query_type": policy_data["query_type"],
    }


def get_policy_data(
    select_policies: Sequence[AllocationPolicy],
    delete_policies: Sequence[AllocationPolicy],
) -> list[PolicyData]:
    policies_data = []
    for policy in chain(select_policies, delete_policies):
        policies_data.append(policy.to_dict())
    return policies_data
