import itertools
from typing import Mapping, NamedTuple, Sequence, Tuple

from snuba.clusters.storage_sets import is_valid_storage_set_combination
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan


class PlanData(NamedTuple):
    rank: int
    alias: str
    plan: ClickhouseQueryPlan


class PlanCandidate:
    def __init__(self, plan_data: Tuple[PlanData, ...]) -> None:
        self.__plan_data = plan_data

    def is_valid(self) -> bool:
        plan_storage_sets = [pd.plan.storage_set_key for pd in self.__plan_data]
        return is_valid_storage_set_combination(*plan_storage_sets)

    def get_rank_sum(self) -> int:
        return sum(pd.rank for pd in self.__plan_data)

    def get_plans_mapping(self) -> Mapping[str, ClickhouseQueryPlan]:
        return {plan_data.alias: plan_data.plan for plan_data in self.__plan_data}


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
    plan_data = [
        [PlanData(rank, alias, plan) for rank, plan in enumerate(_plans)]
        for alias, _plans in plans.items()
    ]

    all = [PlanCandidate(data) for data in itertools.product(*plan_data)]

    candidates = sorted(
        [candidate for candidate in all if candidate.is_valid()],
        key=lambda candidate: candidate.get_rank_sum(),
    )

    assert len(candidates) > 0, "Cannot build a valid query plan for this query."
    return candidates[0].get_plans_mapping()
