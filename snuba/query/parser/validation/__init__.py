import logging
from abc import ABC, abstractmethod
from typing import Optional, Sequence

from snuba.datasets.entity import Entity
from snuba.datasets.entities.factory import get_entity
from snuba.query import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source import DataSource
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Expression
from snuba.query.logical import Query as LogicalQuery

logger = logging.getLogger(__name__)


class ExpressionValidator(ABC):
    """
    Validates an individual expression in a Snuba logical query.
    """

    @abstractmethod
    def validate(self, exp: Expression, data_source: DataSource) -> None:
        """
        If the expression is valid according to this validator it
        returns, otherwise it raises a subclass of
        snuba.query.parser.ValidationException
        """
        raise NotImplementedError


from snuba.query.parser.validation.functions import FunctionCallsValidator

validators: Sequence[ExpressionValidator] = [FunctionCallsValidator()]


def validate_query(query: Query) -> None:
    """
    Applies all the expression validators in one pass over the AST.
    """

    for exp in query.get_all_expressions():
        for v in validators:
            v.validate(exp, query.get_from_clause())


class QueryValidator(ABC):
    """
    Validates logic that requires multiple parts of a query.
    """

    @abstractmethod
    def validate(
        self, entity: Entity, query: Query, alias: Optional[str] = None
    ) -> None:
        """
        For the given entity, validate that the query is correct. If the query
        is not valid, raise an Exception, otherwise return None.

        :param entity: The entity to base the validation on,
        :type entity: Entity
        :param query: The query to validate.
        :type query: Query
        :param alias: The alias of the entity in a JOIN query.
        :type alias: Optional[str]
        :raises InvalidQueryException: [description]
        """
        raise NotImplementedError


from snuba.query.parser.validation.conditions import EntityRequiredColumnValidator

query_validators = [EntityRequiredColumnValidator()]


def validate_entities_with_query(query: Query) -> None:
    if isinstance(query, LogicalQuery):
        entity = get_entity(query.get_from_clause().key)
        for v in query_validators:
            v.validate(entity, query)
    else:
        from_clause = query.get_from_clause()
        if isinstance(from_clause, (LogicalQuery, CompositeQuery)):
            return validate_entities_with_query(from_clause)

        assert isinstance(from_clause, JoinClause)  # mypy
        alias_map = from_clause.get_alias_node_map()
        for alias, node in alias_map.items():
            assert isinstance(node.data_source, QueryEntity)  # mypy
            entity = get_entity(node.data_source.key)
            for v in query_validators:
                v.validate(entity, query, alias)
