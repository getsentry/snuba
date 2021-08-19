import logging
from collections import ChainMap
from typing import Mapping, Optional

from snuba.clickhouse.columns import Array, String
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source import DataSource
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.parser.validation import ExpressionValidator
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall
from snuba.query.validation.signature import Any, Column, SignatureValidator

logger = logging.getLogger(__name__)


default_validators: Mapping[str, FunctionCallValidator] = {
    # like and notLike need to take care of Arrays as well since
    # Arrays are exploded into strings if they are part of the arrayjoin
    # clause.
    # TODO: provide a more restrictive support for arrayjoin.
    "like": SignatureValidator([Column({Array, String}), Any()]),
    "notLike": SignatureValidator([Column({Array, String}), Any()]),
}


class QueryEntityFinder(
    DataSourceVisitor[QueryEntity, QueryEntity], JoinVisitor[QueryEntity, QueryEntity]
):
    """
    Finds the QueryEntity from the data source. The QueryEntity is passed
    through to each FunctionCallValidator (singular):

    ```
        validator.validate(exp.parameters, query_entity)
    ```

    within the validate function below for the FunctionCallsValidator (plural).

    We don't want a FunctionCallValidator (singular) to depend on the
    dataset Entity, because of circular dependencies. However, we _need_
    the Entity in the FunctionCallsValidator (plural) in order to get
    the entity specific validator instances.

    ```
        entity.get_function_call_validators()
    ```

    Hences the reason for having both the QueryEntity and the Entity. :)


    TODO(meredith): Have this return a list of the QueryEntities instead of
    a single QueryEntity.
    """

    def _visit_simple_source(self, data_source: QueryEntity) -> QueryEntity:
        return data_source

    def _visit_join(self, data_source: JoinClause[QueryEntity]) -> QueryEntity:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[QueryEntity]
    ) -> QueryEntity:
        return self.visit(data_source.get_from_clause())

    def _visit_composite_query(
        self, data_source: CompositeQuery[QueryEntity]
    ) -> QueryEntity:
        return self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[QueryEntity]) -> QueryEntity:
        return self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[QueryEntity]) -> QueryEntity:
        # Just returns one entity for now, later return both entities
        return node.left_node.accept(self)


class FunctionCallsValidator(ExpressionValidator):
    """
    Applies all function validators on the provided expression.
    The individual function validators are divided in two mappings:
    a default one applied to all queries and one a mapping per dataset.
    """

    def validate(self, exp: Expression, data_source: DataSource) -> None:
        if not isinstance(exp, FunctionCall):
            return

        if not isinstance(
            data_source, (QueryEntity, JoinClause, CompositeQuery, ProcessableQuery)
        ):
            return

        entity_validators: Mapping[str, FunctionCallValidator] = {}
        entity: Optional[Entity] = None

        query_entity = QueryEntityFinder().visit(data_source)

        if not query_entity:
            return

        entity = get_entity(query_entity.key)
        if not entity:
            return

        entity_validators = entity.get_function_call_validators()

        common_function_validators = (
            entity_validators.keys() & default_validators.keys()
        )
        if common_function_validators:
            logger.warning(
                "Dataset validators are overlapping with default ones. Entity: %s. Overlap %r",
                entity,
                common_function_validators,
                exc_info=True,
            )

        validators = ChainMap(default_validators, entity_validators)
        try:
            # TODO: Decide whether these validators should exist at the Dataset or Entity level
            validator = validators.get(exp.function_name)
            if validator is not None and entity is not None:
                validator.validate(exp.parameters, query_entity)
        except InvalidFunctionCall as exception:
            raise InvalidExpressionException(
                exp,
                f"Illegal call to function {exp.function_name}: {str(exception)}",
                report=False,
            ) from exception
