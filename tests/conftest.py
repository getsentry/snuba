from typing import Any, Callable, Iterator, List, Union

import json
import pytest

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.environment import setup_sentry
from snuba.redis import redis_client
from snuba.utils.clock import Clock, TestingClock
from snuba.utils.streams.backends.local.backend import LocalBroker
from snuba.utils.streams.backends.local.storages.memory import MemoryMessageStorage
from snuba.utils.streams.types import TPayload


def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    assert (
        settings.TESTING
    ), "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

    setup_sentry()

    for cluster in settings.CLUSTERS:
        connection = ClickhousePool(
            cluster["host"], cluster["port"], "default", "", "default",
        )
        database_name = cluster["database"]
        connection.execute(f"DROP DATABASE IF EXISTS {database_name};")
        connection.execute(f"CREATE DATABASE {database_name};")


@pytest.fixture
def clock() -> Iterator[Clock]:
    yield TestingClock()


@pytest.fixture
def broker(clock: TestingClock) -> Iterator[LocalBroker[TPayload]]:
    yield LocalBroker(MemoryMessageStorage(), clock)


@pytest.fixture(autouse=True)
def run_migrations() -> Iterator[None]:
    from snuba.migrations.runner import Runner

    Runner().run_all(force=True)

    yield

    for storage_key in StorageKey:
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
        database = cluster.get_database()

        schema = storage.get_schema()
        if isinstance(schema, WritableTableSchema):
            table_name = schema.get_local_table_name()
            connection.execute(f"TRUNCATE TABLE IF EXISTS {database}.{table_name}")

    redis_client.flushdb()


@pytest.fixture
def convert_legacy_to_snql() -> Iterator[Callable[[str, str], str]]:
    def convert(data: str, entity: str) -> str:
        legacy = json.loads(data)

        def func(value: Union[str, List[Any]]) -> str:
            if not isinstance(value, list):
                return f"{value}" if value is not None else "NULL"

            children = ""
            if isinstance(value[1], list):
                children = ",".join(map(func, value[1]))
            elif value[1]:
                children = func(value[1])

            alias = f" AS {value[2]}" if len(value) > 2 else ""
            return f"{value[0]}({children}){alias}"

        def literal(value: Union[str, List[Any]]) -> str:
            if isinstance(value, list):
                return f"tuple({','.join(list(map(literal, value)))})"

            try:
                float(value)
                return f"{value}"
            except ValueError:
                escaped = value.replace("'", "\\'")
                return f"'{escaped}'"

        sample = legacy.get("sample")
        sample_clause = f"SAMPLE {sample}" if sample else ""
        match_clause = f"MATCH ({entity} {sample_clause})"

        selected = ", ".join(map(func, legacy.get("selected_columns", [])))
        select_clause = f"SELECT {selected}" if selected else ""

        arrayjoin = legacy.get("arrayjoin")
        if arrayjoin:
            array_join_clause = (
                f"arrayJoin({arrayjoin}) AS {arrayjoin}" if arrayjoin else ""
            )
            select_clause = (
                f"SELECT {array_join_clause}"
                if not select_clause
                else f"{select_clause}, {array_join_clause}"
            )

        aggregations = []
        for a in legacy.get("aggregations", []):
            if a[0].endswith(")") and not a[1]:
                aggregations.append(f"{a[0]} AS {a[2]}")
            else:
                agg = func(a)
                aggregations.append(agg)

        aggregations_str = ", ".join(aggregations)
        joined = ", " if select_clause else "SELECT "
        aggregation_clause = f"{joined}{aggregations_str}" if aggregations_str else ""

        groupby = legacy.get("groupby", [])
        if groupby and not isinstance(groupby, list):
            groupby = [groupby]

        groupby = ", ".join(map(func, groupby))
        groupby_clause = f"BY {groupby}" if groupby else ""

        word_ops = ("NOT IN", "IN", "LIKE", "NOT LIKE")
        conditions = []
        for cond in legacy.get("conditions", []):
            if len(cond) != 3 or not isinstance(cond[1], str):
                or_condition = []
                for or_cond in cond:
                    op = f" {or_cond[1]} " if or_cond[1] in word_ops else or_cond[1]
                    or_condition.append(
                        f"{func(or_cond[0])}{op}{literal(or_cond[2])}".join(or_cond)
                    )
                or_condition_str = " OR ".join(or_condition)
                conditions.append(f"{or_condition_str}")
            else:
                op = f" {cond[1]} " if cond[1] in word_ops else cond[1]
                conditions.append(f"{func(cond[0])}{op}{literal(cond[2])}")

        project = legacy.get("project")
        if isinstance(project, int):
            conditions.append(f"project_id={project}")
        elif isinstance(project, list):
            project = ",".join(map(str, project))
            conditions.append(f"project_id IN {project}")

        organization = legacy.get("organization")
        if isinstance(organization, int):
            conditions.append(f"org_id={organization}")
        elif isinstance(organization, list):
            organization = ",".join(organization)
            conditions.append(f"org_id IN {organization}")

        # Hack to help keep legacy in step with the validation SnQL requires
        required_conds = get_entity(EntityKey(entity)).get_required_conditions()
        time_column = None
        for c, ops in required_conds.items():
            if len(ops) > 2:  # Only the time conditions have more than 2
                time_column = c
                break

        if time_column:
            time_cols = (("from_date", ">="), ("to_date", "<"))
            for col, op in time_cols:
                date_val = legacy.get(col)
                if date_val:
                    conditions.append(f"{time_column} {op} toDateTime('{date_val}')")

        conditions_str = " AND ".join(conditions)
        where_clause = f"WHERE {conditions_str}" if conditions_str else ""

        having = []
        for cond in legacy.get("having", []):
            if len(cond) != 3 or not isinstance(cond[1], str):
                or_condition = []
                for or_cond in cond:
                    op = f" {or_cond[1]} " if or_cond[1] in word_ops else or_cond[1]
                    or_condition.append(
                        f"{func(or_cond[0])}{op}{literal(or_cond[2])}".join(or_cond)
                    )
                or_condition_str = " OR ".join(or_condition)
                having.append(f"{or_condition_str}")
            else:
                op = f" {cond[1]} " if cond[1] in word_ops else cond[1]
                having.append(f"{func(cond[0])}{op}{literal(cond[2])}")

        having_str = " AND ".join(having)
        having_clause = f"HAVING {having_str}" if having_str else ""

        order_by = legacy.get("orderby")
        order_by_str = ""
        if order_by:
            if isinstance(order_by, list):
                parts: List[str] = []
                for part in order_by:
                    sort = "ASC"
                    if part.startswith("-"):
                        part = part[1:]
                        sort = "DESC"

                    parts.append(f"{part} {sort}")
                order_by_str = ",".join(parts)
            else:
                sort = "ASC"
                if order_by.startswith("-"):
                    order_by = order_by[1:]
                    sort = "DESC"
                order_by_str = f"{order_by} {sort}"
        order_by_clause = f"ORDER BY {order_by_str}" if order_by else ""

        limit_by_clause = ""
        if legacy.get("limitby"):
            limit, column = legacy.get("limitby")
            limit_by_clause = f"LIMIT {limit} BY {column}"

        extras = ("limit", "offset", "granularity", "totals")
        extra_exps = []
        for extra in extras:
            if legacy.get(extra):
                extra_exps.append(f"{extra.upper()} {legacy.get(extra)}")
        extras_clause = " ".join(extra_exps)

        query = f"{match_clause} {select_clause} {aggregation_clause} {groupby_clause} {where_clause} {having_clause} {order_by_clause} {limit_by_clause} {extras_clause}"
        body = {"query": query}

        return json.dumps(body)

    yield convert
