from typing import Any

from snuba.query.allocation_policies import PolicyData


def convert(policy_data: PolicyData) -> dict[str, Any]:
    "We need to convert the policy data to the format that the frontend expects. This will be removed once we update the frontend"
    return {
        "policy_name": policy_data["configurable_component_config_key"],
        "configs": policy_data["configurations"],
        "optional_config_definitions": policy_data["optional_config_definitions"],
        "query_type": policy_data["query_type"],
    }
