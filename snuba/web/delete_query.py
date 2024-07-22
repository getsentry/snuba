from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import combine_and_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import column, equals, in_cond, literal, literals_tuple


def construct_condition(body):
    and_conditions = []
    for col, values in body.get("columns").items():
        if len(values) == 1:
            exp = equals(column(col), literal(values[0]))
        else:
            literal_values = [literal(v) for v in values]
            exp = in_cond(column(col), literals_tuple(literal_values))

        and_conditions.append(exp)

    return combine_and_conditions(and_conditions)


def delete_query(storage: StorageKey, table: str, body):
    query = Query(
        from_clause=Table(
            table,
            None,
            storage_key=storage,
            allocation_policies=[],
        ),
        condition=construct_condition(body),
        is_delete=True,
    )
    formatted_query = format_query(query)
    # TODO error handling and the lot
    return storage.get_cluster().get_deleter().execute(formatted_query)
