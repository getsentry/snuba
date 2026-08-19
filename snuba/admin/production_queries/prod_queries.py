from typing import Any

from flask import Response

from snuba import settings
from snuba.admin.audit_log.query import audit_log
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    ConditionFunctions,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
    is_in_condition_pattern,
)
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.projects_finder import ProjectsFinder
from snuba.query.data_source.simple import LogicalDataSource
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Expression
from snuba.query.matchers import Any as AnyMatcher
from snuba.query.matchers import Column, FunctionCall, Literal, String
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer
from snuba.web.views import dataset_query


def run_snql_query(body: dict[str, Any], user: str) -> Response:
    """
    Validates, audit logs, and executes given query.
    """

    @audit_log
    def run_query_with_audit(query: str, user: str) -> Response:
        dataset_name = body.pop("dataset")
        dataset = get_dataset(dataset_name)
        body["dry_run"] = True
        response = dataset_query(dataset_name, body, Timer("admin"))
        if response.status_code != 200:
            return response

        body["dry_run"] = False
        _validate_projects_in_query(body, dataset)
        return dataset_query(dataset_name, body, Timer("admin"))

    return run_query_with_audit(body["query"], user)


def _validate_projects_in_query(body: dict[str, Any], dataset: Dataset) -> None:
    """
    Validates that the projects accessed by the query are allowed to be accessed.
    """
    # In debug, we don't need to validate projects
    if settings.DEBUG and len(settings.ADMIN_ALLOWED_PROD_PROJECTS) == 0:
        return

    request_parts = RequestSchema.build(HTTPQuerySettings).validate(body)
    query = parse_snql_query(request_parts.query["query"], dataset)
    if _or_branch_missing_project_id_in_query(query):
        raise InvalidQueryException("Every OR branch must constrain project_id")
    project_ids = ProjectsFinder().visit(query)

    if project_ids == set():
        raise InvalidQueryException("Missing project ID")

    disallowed_project_ids = project_ids.difference(set(settings.ADMIN_ALLOWED_PROD_PROJECTS))
    if len(disallowed_project_ids) > 0:
        raise InvalidQueryException(
            f"Cannot access the following project ids: {disallowed_project_ids}"
        )


def _or_branch_missing_project_id_in_query(
    query: ProcessableQuery[LogicalDataSource] | CompositeQuery[LogicalDataSource],
) -> bool:
    return _OrBranchMissingProjectId().visit(query)


def _condition_is_project_id_eq_or_in(condition: Expression) -> bool:
    if FunctionCall(
        String(ConditionFunctions.EQ),
        (
            Column(column_name=String("project_id")),
            Literal(value=AnyMatcher(int)),
        ),
    ).match(condition):
        return True
    return (
        is_in_condition_pattern(Column(column_name=String("project_id"))).match(condition)
        is not None
    )


def _condition_has_or(condition: Expression) -> bool:
    or_branches = get_first_level_or_conditions(condition)
    if len(or_branches) > 1:
        return True
    and_terms = get_first_level_and_conditions(or_branches[0])
    if len(and_terms) == 1:
        return False
    return any(_condition_has_or(term) for term in and_terms)


def _condition_constrains_project_id(condition: Expression) -> bool:
    or_branches = get_first_level_or_conditions(condition)
    if len(or_branches) > 1:
        return all(_condition_constrains_project_id(branch) for branch in or_branches)
    and_terms = get_first_level_and_conditions(or_branches[0])
    if len(and_terms) > 1:
        return any(_condition_constrains_project_id(term) for term in and_terms)
    return _condition_is_project_id_eq_or_in(and_terms[0])


def _or_branch_missing_project_id(condition: Expression | None) -> bool:
    if condition is None:
        return False
    return _condition_has_or(condition) and not _condition_constrains_project_id(condition)


class _OrBranchMissingProjectId(
    DataSourceVisitor[bool, LogicalDataSource],
    JoinVisitor[bool, LogicalDataSource],
):
    """
    True if any OR branch in the query tree lacks a project_id EQ/IN constraint.
    """

    def _visit_simple_source(self, data_source: LogicalDataSource) -> bool:
        return False

    def _visit_join(self, data_source: JoinClause[LogicalDataSource]) -> bool:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[LogicalDataSource]) -> bool:
        return _or_branch_missing_project_id(data_source.get_condition())

    def _visit_composite_query(self, data_source: CompositeQuery[LogicalDataSource]) -> bool:
        if _or_branch_missing_project_id(data_source.get_condition()):
            return True
        return self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[LogicalDataSource]) -> bool:
        return self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[LogicalDataSource]) -> bool:
        return node.left_node.accept(self) or node.right_node.accept(self)
