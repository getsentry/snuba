import typing
from typing import Any, Dict

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.conditions import combine_and_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import column, equals, in_cond, literal, literals_tuple
from snuba.query.exceptions import TooManyDeleteRowsException
from snuba.query.expressions import Expression, FunctionCall
from snuba.reader import Result
from snuba.state import get_config
from snuba.utils.metrics.util import with_span


@with_span()
def delete_from_storage(
    storage: WritableTableStorage, columns: Dict[str, list[Any]]
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

    Deletes all rows in the given storage, that satisfy the conditions
    defined in 'columns' input.

    Returns a mapping from clickhouse table name to deletion results, there
    will be an entry for every local clickhouse table that makes up the storage.
    """
    if get_config("storage_deletes_enabled", 0):
        raise Exception("Deletes not enabled")

    delete_settings = storage.get_deletion_settings()
    if not delete_settings.is_enabled:
        raise Exception(f"Deletes not enabled for {storage.get_storage_key().value}")

    results: dict[str, Result] = {}
    for table in delete_settings.tables:
        result = _delete_from_table(storage, table, columns)
        results[table] = result
    return results


def _get_rows_to_delete(
    storage_key: StorageKey, select_query_to_count_rows: Query
) -> int:
    formatted_select_query_to_count_rows = format_query(select_query_to_count_rows)
    select_query_results = (
        get_storage(storage_key)
        .get_cluster()
        .get_reader()
        .execute(formatted_select_query_to_count_rows)
    )
    return typing.cast(int, select_query_results["data"][0]["count"])


def _enforce_max_rows(delete_query: Query) -> None:
    """
    The cost of a lightweight delete operation depends on the number of matching rows in the WHERE clause and the current number of data parts.
    This operation will be most efficient when matching a small number of rows, **and on wide parts** (where the `_row_exists` column is stored
    in its own file)

    Because of the above, we want to limit the number of rows one deletes at a time. The `MaxRowsEnforcer` will query clickhouse to see how many
      rows we plan on deleting and if it crosses the `max_rows_to_delete` set for that storage we will reject the query.
    """
    select_query_to_count_rows = Query(
        selected_columns=[
            SelectedExpression("count", FunctionCall("count", "count", ())),
        ],
        from_clause=delete_query.get_from_clause(),
        condition=delete_query.get_condition(),
    )
    storage_key = delete_query.get_from_clause().storage_key
    rows_to_delete = _get_rows_to_delete(
        storage_key=storage_key, select_query_to_count_rows=select_query_to_count_rows
    )
    max_rows_allowed = (
        get_storage(storage_key).get_deletion_settings().max_rows_to_delete
    )
    if rows_to_delete > max_rows_allowed:
        raise TooManyDeleteRowsException(
            f"Too many rows to delete ({rows_to_delete}), maximum allowed is {max_rows_allowed}"
        )


def _delete_from_table(
    storage: WritableTableStorage, table: str, conditions: Dict[str, Any]
) -> Result:
    cluster_name = storage.get_cluster().get_clickhouse_cluster_name()
    on_cluster = literal(cluster_name) if cluster_name else None
    query = Query(
        from_clause=Table(
            table,
            ColumnSet([]),
            storage_key=storage.get_storage_key(),
            # TODO: add allocation policies
            allocation_policies=[],
        ),
        condition=_construct_condition(conditions),
        on_cluster=on_cluster,
        is_delete=True,
    )
    _enforce_max_rows(query)

    formatted_query = format_query(query)
    # TODO error handling and the lot
    return storage.get_cluster().get_deleter().execute(formatted_query)


def _construct_condition(columns: Dict[str, Any]) -> Expression:
    and_conditions = []
    for col, values in columns.items():
        if len(values) == 1:
            exp = equals(column(col), literal(values[0]))
        else:
            literal_values = [literal(v) for v in values]
            exp = in_cond(
                column(col), literals_tuple(alias=None, literals=literal_values)
            )

        and_conditions.append(exp)

    return combine_and_conditions(and_conditions)
