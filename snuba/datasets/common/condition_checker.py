from abc import ABC, abstractmethod

from snuba.query.expressions import Expression


class ConditionChecker(ABC):
    """
    Checks if an expression matches a specific shape and content.

    These are declared by storages as mandatory conditions that are
    supposed to be in the query before it is executed for the query
    to be acceptable.

    This system is meant to be a failsafe mechanism to prevent
    bugs in any step of query processing to generate queries that are
    missing project_id and org_id conditions from the query.
    """

    @abstractmethod
    def get_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def check(self, expression: Expression) -> bool:
        raise NotImplementedError
