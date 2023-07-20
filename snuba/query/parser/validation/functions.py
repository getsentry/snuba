from typing import List, Mapping

from snuba.clickhouse.columns import Array, DateTime, String
from snuba.datasets.entities.factory import get_entity
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
from snuba.query.validation.functions import AllowedFunctionValidator
from snuba.query.validation.signature import Any, Column, SignatureValidator

DateTimeValidator = SignatureValidator([Column({DateTime})], enforce=False)

default_validators: Mapping[str, FunctionCallValidator] = {
    "toStartOfMinute": DateTimeValidator,
    "toStartOfHour": DateTimeValidator,
    "toStartOfDay": DateTimeValidator,
    "toStartOfYear": DateTimeValidator,
    # Techincally this can be called with either a datetime or string
    # but seems okay to be more restrictive for now
    "toUnixTimestamp": DateTimeValidator,
    # like and notLike need to take care of Arrays as well since
    # Arrays are exploded into strings if they are part of the arrayjoin
    # clause.
    # TODO: provide a more restrictive support for arrayjoin.
    "like": SignatureValidator([Column({Array, String}), Any()]),
    "notLike": SignatureValidator([Column({Array, String}), Any()]),
}
global_validators: List[FunctionCallValidator] = [AllowedFunctionValidator()]


class QueryEntityFinder(
    DataSourceVisitor[List[QueryEntity], QueryEntity],
    JoinVisitor[List[QueryEntity], QueryEntity],
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

    def _visit_simple_source(self, data_source: QueryEntity) -> List[QueryEntity]:
        return [data_source]

    def _visit_join(self, data_source: JoinClause[QueryEntity]) -> List[QueryEntity]:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[QueryEntity]
    ) -> List[QueryEntity]:
        return self.visit(data_source.get_from_clause())

    def _visit_composite_query(
        self, data_source: CompositeQuery[QueryEntity]
    ) -> List[QueryEntity]:
        return []

    def visit_individual_node(
        self, node: IndividualNode[QueryEntity]
    ) -> List[QueryEntity]:
        return self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[QueryEntity]) -> List[QueryEntity]:
        return node.right_node.accept(self) + node.left_node.accept(self)


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

        # First do global validation, independent of entities
        try:
            for validator in global_validators:
                validator.validate(exp.function_name, exp.parameters, data_source)
        except InvalidFunctionCall as exception:
            raise InvalidExpressionException.from_args(
                exp,
                f"Illegal call to function {exp.function_name}: {str(exception)}",
                should_report=False,
            ) from exception

        # Then do entity specific validation, if there are any
        entity = None
        query_entities = QueryEntityFinder().visit(data_source)

        for query_entity in query_entities:
            entity = get_entity(query_entity.key)

            # TODO(meredith): If there are multiple entities but no
            # entity validators, this will call the default validator
            # multiple times.
            entity_validator = entity.get_function_call_validators().get(
                exp.function_name
            ) or default_validators.get(exp.function_name)

            if not entity_validator:
                return

            try:
                entity_validator.validate(
                    exp.function_name, exp.parameters, data_source
                )
            except InvalidFunctionCall as exception:
                raise InvalidExpressionException.from_args(
                    exp,
                    f"Illegal call to function {exp.function_name}: {str(exception)}",
                    should_report=False,
                ) from exception
