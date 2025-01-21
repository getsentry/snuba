from __future__ import annotations

from enum import Enum


class ReadinessState(Enum):
    """
    Readiness states are essentially feature flags for snuba datasets.
    The readiness state defines whether or not a dataset is made available
    in specific sentry environments.
    Currently, sentry environments include the following:
    * local/CI
    * SaaS
    * S4S
    * Self-Hosted
    * Single-Tenant

    The following is a list of readiness states and the environments
    they map to:
    * limited -> local/CI
    * experimental -> local/CI, S4S
    * partial -> local/CI, SaaS, S4S
    * deprecate -> local/CI, Self-Hosted
    * complete ->  local/CI, SaaS, S4S, Self-Hosted, Single-Tenant
    """

    LIMITED = "limited"
    DEPRECATE = "deprecate"
    PARTIAL = "partial"
    COMPLETE = "complete"
    EXPERIMENTAL = "experimental"

    def __init__(self, _: str) -> None:
        self.order = {
            "limited": 1,
            "deprecate": 2,
            "experimental": 3,
            "partial": 4,
            "complete": 5,
        }

    def __gt__(self, other_rs: ReadinessState) -> bool:
        return self.order[self.value] > self.order[other_rs.value]

    def __lt__(self, other_rs: ReadinessState) -> bool:
        return self.order[self.value] < self.order[other_rs.value]
