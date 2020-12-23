from dataclasses import dataclass, field
from typing import Mapping, MutableMapping, Sequence, Tuple

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan


@dataclass
class StorageSetPlansCandidate:
    """
    Keeps track of the best plan encountered for a storage set and
    keeps a total cost, which is the sum of the ranking order of
    each plan.
    """

    total_cost: int = 0
    plans: MutableMapping[str, Tuple[ClickhouseQueryPlan, int]] = field(
        default_factory=dict
    )

    def add_plan(self, alias: str, plan: ClickhouseQueryPlan, cost: int) -> None:
        if alias not in self.plans or self.plans[alias][1] > cost:
            self.plans[alias] = (plan, cost)
            self.total_cost = sum([plan[1] for plan in self.plans.values()])

    def is_complete(self, expected_aliases: int) -> bool:
        return len(self.plans) == expected_aliases

    def get_plans_mapping(self) -> Mapping[str, ClickhouseQueryPlan]:
        return {alias: plan[0] for alias, plan in self.plans.items()}


def select_best_plans(
    plans: Mapping[str, Sequence[ClickhouseQueryPlan]]
) -> Mapping[str, ClickhouseQueryPlan]:
    """
    Receives a mapping with all the valid query plans for each subquery
    in a join. These plans are supposed to be in a sequence sorted by
    ranking.

    It selects a plan for each subquery (each table alias) ensuring that
    all the selected plans are in the same storage set and that the sum
    of the ranking is minimal.
    """

    ranks: MutableMapping[StorageSetKey, StorageSetPlansCandidate] = {}
    for alias, viable_plans in plans.items():
        for rank, plan in enumerate(viable_plans):
            if plan.storage_set_key not in ranks:
                ranks[plan.storage_set_key] = StorageSetPlansCandidate()
            ranks[plan.storage_set_key].add_plan(alias, plan, rank)

    candidates = sorted(
        [r for r in ranks.values() if r.is_complete(len(plans))],
        key=lambda candidate: candidate.total_cost,
    )

    return candidates[0].get_plans_mapping()
