from typing import Any, Sequence, Tuple, cast

from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage

# from snuba.admin.views import application

# TODO (Vlad): we have to decouple getting a cluster from getting a connection
# for now though, I'm just going to make it so you can query the query node


def run_query(
    clickhouse_host: str, storage_name: str, sql: str
) -> Tuple[Sequence[Any], Sequence[Tuple[str, str]]]:
    clickhouse_port = 9000
    # TODO: don't create a new pool every time, that's no good
    storage_key = StorageKey(storage_name)
    storage = get_storage(storage_key)
    (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()
    database = storage.get_cluster().get_database()
    connection = ClickhousePool(
        clickhouse_host, clickhouse_port, clickhouse_user, clickhouse_password, database
    )
    return cast(
        Tuple[Sequence[Any], Sequence[Tuple[str, str]]],
        connection.execute(query=sql, with_column_types=True),
    )
