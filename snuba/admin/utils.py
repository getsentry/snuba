from typing import Sequence

from snuba.configs.configuration import ConfigurableComponentData
from snuba.query.allocation_policies import AllocationPolicy


def get_policy_data(
    select_policies: Sequence[AllocationPolicy],
    delete_policies: Sequence[AllocationPolicy],
) -> list[ConfigurableComponentData]:
    policies_data = []
    for policy in select_policies:
        policies_data.append(policy.to_dict(additional_data={"query_type": "select"}))
    for policy in delete_policies:
        policies_data.append(policy.to_dict(additional_data={"query_type": "delete"}))
    return policies_data
