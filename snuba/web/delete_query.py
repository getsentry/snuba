from typing import Any, Dict

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.datasets.storage import WritableTableStorage
from snuba.query.conditions import combine_and_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import column, equals, in_cond, literal, literals_tuple
from snuba.query.expressions import Expression
from snuba.reader import Result


def construct_condition(body: Dict[str, Any]) -> Expression:
    and_conditions = []
    for col, values in body["columns"].items():
        if len(values) == 1:
            exp = equals(column(col), literal(values[0]))
        else:
            literal_values = [literal(v) for v in values]
            exp = in_cond(
                column(col), literals_tuple(alias=None, literals=literal_values)
            )

        and_conditions.append(exp)

    return combine_and_conditions(and_conditions)


def delete_query(
    storage: WritableTableStorage, table: str, body: Dict[str, Any]
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
        condition=construct_condition(body),
        on_cluster=on_cluster,
        is_delete=True,
    )
    formatted_query = format_query(query)
    # TODO error handling and the lot
    return storage.get_cluster().get_deleter().execute(formatted_query)
