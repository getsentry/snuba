from abc import ABC, abstractmethod

from snuba.datasets.plans.query_plan import QueryPlanExecutionStrategy
from snuba.request import Request


class StorageQuerySplitStrategy(QueryPlanExecutionStrategy, ABC):
    """
    An execution strategy that implements one query split algorithm. For example
    a StorageQuerySplitStrategy can implement the time based query splitting.
    Since not every split algorithm can work on every query, a StorageQuerySplitStrategy
    has a validation method "can_execute" that the StorageQueryPlanBuilder uses
    to select the valid algorithm.
    """

    @abstractmethod
    def can_execute(self, request: Request) -> bool:
        """
        Returns True if this split algorithm can be applied to the Query provided.
        """
        raise NotImplementedError
