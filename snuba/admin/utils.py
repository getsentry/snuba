from typing import Any, Sequence, TypedDict

from snuba.query.allocation_policies import AllocationPolicy

# class ConfigItem(TypedDict):
#     name: str
#     type: str
#     default: Any
#     description: str
#     # For optional configs: params is a list of param definitions
#     # For live configs: params is a dict of actual values
#     params: Union[List[dict[str, Any]], dict[str, Any]]
#     # Only present in live configs
#     value: NotRequired[Any]


class ConfigurableComponentData(TypedDict):
    configs: list[dict[str, Any]]
    optional_config_definitions: list[dict[str, Any]]


class PolicyData(ConfigurableComponentData):
    policy_name: str
    query_type: str


class StrategyData(ConfigurableComponentData):
    strategy_name: str


def add_policy_data(
    policies: Sequence[AllocationPolicy], query_type: str, data: list[PolicyData]
) -> None:
    for policy in policies:
        data.append(
            PolicyData(
                policy_name=policy.config_key(),
                configs=policy.get_current_configs(),
                optional_config_definitions=policy.get_optional_config_definitions_json(),
                query_type=query_type,
            )
        )
