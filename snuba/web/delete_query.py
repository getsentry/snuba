import typing
import uuid
from typing import Any, Mapping, MutableMapping, Optional, Sequence

from snuba import settings
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyViolations,
    QueryResultOrError,
)
from snuba.query.conditions import combine_and_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import column, equals, in_cond, literal, literals_tuple
from snuba.query.exceptions import (
    InvalidQueryException,
    NoRowsToDeleteException,
    TooManyDeleteRowsException,
)
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.query_settings import HTTPQuerySettings
from snuba.reader import Result
from snuba.state import get_config, get_int_config
from snuba.utils.metrics.util import with_span
from snuba.utils.schemas import ColumnValidator, InvalidColumnType
from snuba.web import QueryException, QueryExtraData, QueryResult
from snuba.web.db_query import _apply_allocation_policies_quota

ConditionsType = Mapping[str, Sequence[str | int | float]]


class DeletesNotEnabledError(Exception):
    pass


class TooManyOngoingMutationsError(Exception):
    pass


@with_span()
def delete_from_storage(
    storage: WritableTableStorage,
    columns: ConditionsType,
    attribution_info: Mapping[str, Any],
) -> dict[str, Result]:
    """
    Inputs:
        storage - storage to delete from
        columns - a mapping from column-name to a list of column values
            that defines the delete conditions. ex:
            {
                "id": [1, 2, 3]
                "status": ["failed"]
            }
            represents
            DELETE FROM ... WHERE id in (1,2,3) AND status='failed'
        attribution_info - see other parts of repo
        max_ongoing_mutations - the max number of mutations that can be in-progress
            on any replica in the clickhouse cluster for the delete to be scheduled.
            If any replica in the cluster has more than max_ongoing_mutations in progress,
            then the delete will fail and wont be scheduled.

    Deletes all rows in the given storage, that satisfy the conditions
    defined in 'columns' input.

    Returns a mapping from clickhouse table name to deletion results, there
    will be an entry for every local clickhouse table that makes up the storage.
    """
    if not deletes_are_enabled():
        raise DeletesNotEnabledError("Deletes not enabled in this region")

    delete_settings = storage.get_deletion_settings()
    if not delete_settings.is_enabled:
        raise DeletesNotEnabledError(f"Deletes not enabled for {storage.get_storage_key().value}")

    if delete_settings.bulk_delete_only:
        raise DeletesNotEnabledError(
            f"Synchronous deletes not enabled for {storage.get_storage_key().value}"
        )

    results: dict[str, Result] = {}
    attr_info = _get_attribution_info(attribution_info)

    # fail if too many mutations ongoing
    ongoing_mutations = _num_ongoing_mutations(storage.get_cluster(), delete_settings.tables)
    max_ongoing_mutations = get_int_config(
        "MAX_ONGOING_MUTATIONS_FOR_DELETE",
        default=settings.MAX_ONGOING_MUTATIONS_FOR_DELETE,
    )
    assert max_ongoing_mutations
    if ongoing_mutations > max_ongoing_mutations:
        raise TooManyOngoingMutationsError(
            f"max ongoing mutations to do a delete is {max_ongoing_mutations}, but at least one replica has {ongoing_mutations} ongoing"
        )

    for table in delete_settings.tables:
        result = _delete_from_table(storage, table, columns, attr_info)
        results[table] = result
    return results


def _delete_from_table(
    storage: WritableTableStorage,
    table: str,
    conditions: ConditionsType,
    attribution_info: AttributionInfo,
) -> Result:
    cluster_name = storage.get_cluster().get_clickhouse_cluster_name()
    on_cluster = literal(cluster_name) if cluster_name else None
    query = Query(
        from_clause=Table(
            table,
            ColumnSet([]),
            storage_key=storage.get_storage_key(),
            allocation_policies=storage.get_delete_allocation_policies(),
        ),
        condition=_construct_condition(conditions),
        on_cluster=on_cluster,
        is_delete=True,
    )

    columns = storage.get_schema().get_columns()
    column_validator = ColumnValidator(columns)
    try:
        for col, values in conditions.items():
            column_validator.validate(col, values)
    except InvalidColumnType as e:
        raise InvalidQueryException(e.message)

    try:
        _enforce_max_rows(query)
    except NoRowsToDeleteException:
        result: Result = {}
        return result

    deletion_processors = storage.get_deletion_processors()
    # These settings aren't needed at the moment
    dummy_query_settings = HTTPQuerySettings()
    for deletion_procesor in deletion_processors:
        deletion_procesor.process_query(query, dummy_query_settings)

    return _execute_query(
        query, storage, table, cluster_name, attribution_info, dummy_query_settings
    )


def _num_ongoing_mutations(cluster: ClickhouseCluster, tables: Sequence[str]) -> int:
    """
    Given a clickhouse cluster and a list of tables,
    returns the maximum of ongoing mutations for the tables,
    across all replicas
    """
    if cluster.is_single_node():
        query = f"""
SELECT max(cnt)
FROM (
    SELECT table, count() as cnt
    FROM system.mutations
    WHERE table IN ({", ".join(map(repr, tables))}) AND is_done=0
    GROUP BY table
)
"""
    else:
        query = f"""
SELECT max(cnt)
FROM (
    SELECT hostname() as host, table, count() as cnt
    FROM clusterAllReplicas('{cluster.get_clickhouse_cluster_name()}', 'system', mutations)
    WHERE table IN ({", ".join(map(repr, tables))}) AND is_done=0
    GROUP BY host, table
    )
"""
    return int(
        cluster.get_query_connection(ClickhouseClientSettings.QUERY).execute(query).results[0][0]
    )


def deletes_are_enabled() -> bool:
    return bool(get_config("storage_deletes_enabled", 1))


def _get_rows_to_delete(storage_key: StorageKey, select_query_to_count_rows: Query) -> int:
    formatted_select_query_to_count_rows = format_query(select_query_to_count_rows)
    select_query_results = (
        get_storage(storage_key)
        .get_cluster()
        .get_reader()
        .execute(formatted_select_query_to_count_rows)
    )
    return typing.cast(int, select_query_results["data"][0]["count"])


def _enforce_max_rows(delete_query: Query) -> int:
    """
    The cost of a lightweight delete operation depends on the number of matching rows in the WHERE clause and the current number of data parts.
    This operation will be most efficient when matching a small number of rows, **and on wide parts** (where the `_row_exists` column is stored
    in its own file)

    Because of the above, we want to limit the number of rows one deletes at a time. The `MaxRowsEnforcer` will query clickhouse to see how many
      rows we plan on deleting and if it crosses the `max_rows_to_delete` set for that storage we will reject the query.
    """
    storage_key = delete_query.get_from_clause().storage_key

    def get_new_from_clause() -> Table:
        """
        The delete query targets the local table, but when we are checking the
        row count we are querying the dist tables (if applicable). This function
        updates the from_clause to have the correct table.
        """
        dist_table_name = (
            get_writable_storage((storage_key)).get_table_writer().get_schema().get_table_name()
        )
        from_clause = delete_query.get_from_clause()
        return Table(
            table_name=dist_table_name,
            schema=from_clause.schema,
            storage_key=from_clause.storage_key,
            allocation_policies=from_clause.allocation_policies,
        )

    select_query_to_count_rows = Query(
        selected_columns=[
            SelectedExpression("count", FunctionCall("count", "count", ())),
        ],
        from_clause=get_new_from_clause(),
        condition=delete_query.get_condition(),
    )
    rows_to_delete = _get_rows_to_delete(
        storage_key=storage_key, select_query_to_count_rows=select_query_to_count_rows
    )
    if rows_to_delete == 0:
        raise NoRowsToDeleteException
    max_rows_allowed = get_storage(storage_key).get_deletion_settings().max_rows_to_delete
    if rows_to_delete > max_rows_allowed:
        raise TooManyDeleteRowsException(
            f"Too many rows to delete ({rows_to_delete}), maximum allowed is {max_rows_allowed}"
        )

    return rows_to_delete


def _get_attribution_info(attribution_info: Mapping[str, Any]) -> AttributionInfo:
    info = dict(attribution_info)
    info["app_id"] = get_app_id(attribution_info["app_id"])
    info["referrer"] = attribution_info["referrer"]
    info["tenant_ids"] = attribution_info["tenant_ids"]
    return AttributionInfo(**info)


def _get_delete_allocation_policies(
    storage: WritableTableStorage,
) -> list[AllocationPolicy]:
    """mostly here to be able to stub easily in tests"""
    return storage.get_delete_allocation_policies()


def _execute_query(
    query: Query,
    storage: WritableTableStorage,
    table: str,
    cluster_name: Optional[str],
    attribution_info: AttributionInfo,
    query_settings: HTTPQuerySettings,
) -> Result:
    """
    Formats and executes the delete query, taking into account
    the delete allocation policies as well.
    """

    formatted_query = format_query(query)
    allocation_policies = _get_delete_allocation_policies(storage)
    query_id = uuid.uuid4().hex
    query_settings.push_clickhouse_setting("query_id", query_id)
    result = None
    error = None

    stats: MutableMapping[str, Any] = {
        "clickhouse_table": table,
        "referrer": attribution_info.referrer,
        "cluster_name": cluster_name or "<unknown>",
    }

    try:
        _apply_allocation_policies_quota(
            query_settings,
            attribution_info,
            formatted_query,
            stats,
            allocation_policies,
            query_id,
        )
        result = (
            storage.get_cluster()
            .get_deleter()
            .execute(formatted_query, query_settings.get_clickhouse_settings())
        )
    except AllocationPolicyViolations as e:
        error = QueryException.from_args(
            AllocationPolicyViolations.__name__,
            "Query cannot be run due to allocation policies",
            extra={
                "stats": stats,
                "sql": "no sql run",
                "experiments": {},
            },
        )
        error.__cause__ = e
    except QueryException as e:
        error = e
    except Exception as e:
        error = QueryException.from_args(
            # This exception needs to have the message of the cause in it for sentry
            # to pick it up properly
            e.__class__.__name__,
            e.message if isinstance(e, ClickhouseError) else str(e),
            {
                "stats": stats,
                "sql": "",
                "experiments": {},
            },
        )
        error.__cause__ = e
    finally:
        query_result = (
            QueryResult(
                result=result,
                extra=QueryExtraData(stats=stats, sql=formatted_query.get_sql(), experiments={}),
            )
            if result
            else None
        )
        result_or_error = QueryResultOrError(query_result=query_result, error=error)
        for allocation_policy in allocation_policies:
            allocation_policy.update_quota_balance(
                tenant_ids=attribution_info.tenant_ids,
                query_id=query_id,
                result_or_error=result_or_error,
            )
        if result:
            return result
        raise error or Exception("No error or result when running query, this should never happen")


def _construct_condition(columns: ConditionsType) -> Expression:
    and_conditions = []
    for col, values in columns.items():
        if len(values) == 1:
            exp = equals(column(col), literal(values[0]))
        else:
            literal_values = [literal(v) for v in values]
            exp = in_cond(column(col), literals_tuple(alias=None, literals=literal_values))

        and_conditions.append(exp)

    return combine_and_conditions(and_conditions)
